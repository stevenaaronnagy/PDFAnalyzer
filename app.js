require("dotenv").config();
const { App } = require("@slack/bolt");
const Anthropic = require("@anthropic-ai/sdk").default;
const fs = require("fs");
const path = require("path");
const os = require("os");

// ─── Config ────────────────────────────────────────────────────────────────────
const CLAUDE_MODEL = process.env.CLAUDE_MODEL || "claude-haiku-4-5-20251001";
const ALLOWED_CHANNELS = process.env.ALLOWED_CHANNELS
  ? process.env.ALLOWED_CHANNELS.split(",").map((c) => c.trim())
  : [];

// ─── CSV Column Definitions ────────────────────────────────────────────────────
// These match the exact mca_uivision.csv format
const CSV_HEADERS = [
  "Legal Name",
  "DBA",
  "Tax ID",
  "Entity Type",
  "Start Date",
  "Industry",
  "Business Phone",
  "Address",
  "City",
  "State",
  "ZIP",
  "Annual Revenue",
  "Monthly Revenue",
  "Avg Bank Balance",
  "Avg Daily Ledger",
  "Monthly CC Volume",
  "Bankruptcies",
  "Judgments",
  "Tax Liens",
  "Open MCA",
  "Federal Contract",
  "Owner First",
  "Owner Last",
  "SSN",
  "DOB",
  "Email",
  "Home Phone",
  "Cell Phone",
  "Owner Address",
  "Owner City",
  "Owner State",
  "Owner ZIP",
  "Ownership %",
  "Amount Requested",
  "Purpose",
  "Bank Stmts Path",
  "App Path",
  "Time in Business",
  "Start Date (OnDeck)",
  "DOB (OnDeck)",
  "DOB (Headway)",
  "Entity Type (Credibly)",
  "Entity Type (PIRS)",
];

// ─── System Prompt ─────────────────────────────────────────────────────────────
// Cached across all requests to save ~90% on input tokens
const SYSTEM_PROMPT = `You are a data extractor for MobyCap (ATX Funding Source, LLC). You extract fields from MobyCap PDF credit applications into a specific JSON format.

Extract the following fields from the PDF and return ONLY a valid JSON object. No markdown fences, no explanation, no preamble.

Required JSON keys (use exactly these keys):
{
  "legal_name": "Business Legal Name",
  "dba": "Doing Business As",
  "tax_id": "Federal Tax ID / EIN",
  "entity_type_full": "Full entity type (e.g. Limited Liability Company)",
  "entity_type_short": "Short entity type (e.g. LLC, Corp, Sole Prop)",
  "start_date": "Business Start Date (MM/DD/YYYY)",
  "industry": "Industry",
  "business_phone": "Business Phone with area code",
  "address": "Business Street Address",
  "city": "Business City",
  "state": "Business State",
  "zip": "Business ZIP",
  "annual_revenue": "Annual Revenue (numbers with commas, no $)",
  "monthly_revenue": "Monthly Revenue (numbers with commas, no $)",
  "amount_requested": "Amount of Capital Requested (numbers with commas, no $)",
  "purpose": "Purpose of Capital",
  "estimated_credit_score": "Estimated Credit Score",
  "current_advances": "Current Advances & Balances",
  "immediate_capital": "Yes or No",
  "line_of_credit": "Yes or No",
  "invoice_factoring": "Yes or No",
  "equipment_leasing": "Yes or No",
  "term_loan": "Yes or No",
  "owner_first": "Owner First Name",
  "owner_last": "Owner Last Name",
  "owner_ssn": "Owner SSN (format: XXX-XX-XXXX)",
  "owner_dob": "Owner Date of Birth (MM/DD/YYYY)",
  "owner_email": "Owner Email",
  "owner_phone": "Owner Mobile Phone",
  "owner_address": "Owner Home Address",
  "owner_city": "Owner City",
  "owner_state": "Owner State",
  "owner_zip": "Owner ZIP",
  "ownership_pct": "Ownership Percentage (number only)",
  "owner2_first": "2nd Owner First Name (empty if none)",
  "owner2_last": "2nd Owner Last Name (empty if none)",
  "owner2_ssn": "2nd Owner SSN (empty if none)",
  "owner2_dob": "2nd Owner DOB (empty if none)",
  "owner2_email": "2nd Owner Email (empty if none)",
  "owner2_phone": "2nd Owner Phone (empty if none)",
  "owner2_address": "2nd Owner Address (empty if none)",
  "owner2_city": "2nd Owner City (empty if none)",
  "owner2_state": "2nd Owner State (empty if none)",
  "owner2_zip": "2nd Owner ZIP (empty if none)",
  "owner2_ownership_pct": "2nd Owner Ownership % (empty if none)",
  "sign_date": "Date signed"
}

Rules:
- If a field is empty or not present, use an empty string ""
- For checkboxes (funding products), check if they appear to be selected/checked — use "Yes" or "No"
- Split the owner name into first and last name
- For revenue/amounts, use numbers with commas but no dollar sign (e.g. "1,200,000" not "$1,200,000.00")
- For SSN, use format XXX-XX-XXXX (with dashes, no spaces)
- Return ONLY the JSON object`;

// ─── Initialize Slack & Anthropic ──────────────────────────────────────────────
const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  signingSecret: process.env.SLACK_SIGNING_SECRET,
  socketMode: true,
  appToken: process.env.SLACK_APP_TOKEN,
});

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

// ─── Helpers ───────────────────────────────────────────────────────────────────

async function downloadSlackFile(url) {
  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${process.env.SLACK_BOT_TOKEN}` },
  });
  if (!response.ok) {
    throw new Error(`Failed to download file: ${response.status} ${response.statusText}`);
  }
  const buffer = await response.arrayBuffer();
  return Buffer.from(buffer).toString("base64");
}

async function extractFieldsFromPDF(base64Data, fileName) {
  const requestBody = {
    model: CLAUDE_MODEL,
    max_tokens: 4096,
    system: [
      {
        type: "text",
        text: SYSTEM_PROMPT,
        cache_control: { type: "ephemeral" },
      },
    ],
    messages: [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              type: "base64",
              media_type: "application/pdf",
              data: base64Data,
            },
          },
          {
            type: "text",
            text: `Extract all fields from this MobyCap application PDF.\n\nFile name: ${fileName}`,
          },
        ],
      },
    ],
  };

  const message = await anthropic.messages.create(requestBody);
  return parseClaudeResponse(message.content);
}

function parseClaudeResponse(content) {
  const rawText = content
    .filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("\n")
    .trim();

  const cleaned = rawText
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  try {
    return JSON.parse(cleaned);
  } catch (parseError) {
    throw new Error(`Claude returned invalid JSON. Raw response:\n${rawText}`);
  }
}

/**
 * Calculate time in business in years from start date.
 */
function calcTimeInBusiness(startDate) {
  if (!startDate) return "";
  try {
    const parts = startDate.split("/");
    if (parts.length !== 3) return "";
    const start = new Date(parts[2], parts[0] - 1, parts[1]);
    const now = new Date();
    const years = Math.floor((now - start) / (365.25 * 24 * 60 * 60 * 1000));
    return String(years);
  } catch {
    return "";
  }
}

/**
 * Convert MM/DD/YYYY to YYYY-MM-DD (for Headway DOB format).
 */
function toISODate(dateStr) {
  if (!dateStr) return "";
  try {
    const parts = dateStr.split("/");
    if (parts.length !== 3) return "";
    return `${parts[2]}-${parts[0].padStart(2, "0")}-${parts[1].padStart(2, "0")}`;
  } catch {
    return "";
  }
}

/**
 * Convert MM/DD/YYYY to MM-DD-YYYY (for OnDeck format).
 */
function toOnDeckDate(dateStr) {
  if (!dateStr) return "";
  return dateStr.replace(/\//g, "-");
}

/**
 * Parse a revenue string like "1,800,000" or "$1,800,000.00" into a clean number.
 */
function parseRevenue(str) {
  if (!str) return 0;
  return parseFloat(str.replace(/[$,]/g, "")) || 0;
}

/**
 * Format a number with commas (e.g. 1250000 → "1,250,000").
 */
function formatWithCommas(num) {
  if (!num && num !== 0) return "";
  // Handle decimals
  const parts = num.toFixed(2).split(".");
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  // Drop .00 but keep other decimals
  if (parts[1] === "00") return parts[0];
  return parts.join(".");
}

/**
 * State name to 2-letter abbreviation lookup.
 */
const STATE_ABBREVS = {
  "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
  "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
  "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
  "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
  "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
  "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
  "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
  "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
  "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
  "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
  "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
  "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
  "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
};

function toStateAbbrev(state) {
  if (!state) return "";
  // Already a 2-letter abbreviation
  if (state.length === 2 && state === state.toUpperCase()) return state;
  const abbrev = STATE_ABBREVS[state.toLowerCase().trim()];
  return abbrev || state;
}

/**
 * Get short entity type abbreviation.
 */
function getEntityTypeShort(entityType) {
  if (!entityType) return "";
  const lower = entityType.toLowerCase();
  if (lower.includes("limited liability")) return "LLC";
  if (lower.includes("llc")) return "LLC";
  if (lower.includes("s-corp") || lower.includes("s corp")) return "S-Corp";
  if (lower.includes("c-corp") || lower.includes("c corp")) return "C-Corp";
  if (lower.includes("corporation") || lower.includes("corp")) return "Corp";
  if (lower.includes("sole prop")) return "Sole Prop";
  if (lower.includes("partnership")) return "Partnership";
  return entityType;
}

/**
 * Map Claude's extracted JSON to the exact mca_uivision.csv column order.
 * Applies business logic: defaults, calculations, format conversions.
 */
function mapToCSVRow(data) {
  const entityShort = data.entity_type_short || getEntityTypeShort(data.entity_type_full);
  const startDate = data.start_date || "";
  const dob = data.owner_dob || "";
  const legalName = data.legal_name || "";

  // DBA defaults to Legal Name if blank
  const dba = data.dba || legalName;

  // Revenue calculations
  const annualRev = parseRevenue(data.annual_revenue);
  const monthlyRev = data.monthly_revenue ? parseRevenue(data.monthly_revenue) : (annualRev / 12);
  const avgDailyLedger = monthlyRev > 0 ? (monthlyRev / 30) : 0;

  // State abbreviations
  const bizState = toStateAbbrev(data.state);
  const ownerState = toStateAbbrev(data.owner_state);

  return [
    legalName,                                        // Legal Name
    dba,                                              // DBA (defaults to Legal Name)
    data.tax_id || "",                                // Tax ID
    entityShort,                                      // Entity Type
    startDate,                                        // Start Date
    data.industry || "",                              // Industry
    data.business_phone || "",                        // Business Phone
    data.address || "",                               // Address
    data.city || "",                                  // City
    bizState,                                         // State (2-letter)
    data.zip || "",                                   // ZIP
    annualRev ? formatWithCommas(annualRev) : "",     // Annual Revenue
    monthlyRev ? formatWithCommas(monthlyRev) : "",   // Monthly Revenue
    monthlyRev ? formatWithCommas(monthlyRev) : "",   // Avg Bank Balance (= Monthly Revenue)
    avgDailyLedger ? formatWithCommas(avgDailyLedger) : "", // Avg Daily Ledger (Monthly / 30)
    "",                                               // Monthly CC Volume
    "Yes",                                            // Bankruptcies (default Yes)
    "Yes",                                            // Judgments (default Yes)
    "No",                                             // Tax Liens (default No)
    "No",                                             // Open MCA (default No)
    "Yes",                                            // Federal Contract (default Yes)
    data.owner_first || "",                           // Owner First
    data.owner_last || "",                            // Owner Last
    data.owner_ssn || "",                             // SSN
    dob,                                              // DOB
    data.owner_email || "",                           // Email
    data.owner_phone || "",                           // Home Phone
    data.owner_phone || "",                           // Cell Phone
    data.owner_address || "",                         // Owner Address
    data.owner_city || "",                            // Owner City
    ownerState,                                       // Owner State (2-letter)
    data.owner_zip || "",                             // Owner ZIP
    data.ownership_pct || "",                         // Ownership %
    data.amount_requested || "",                      // Amount Requested
    data.purpose || "",                               // Purpose
    "",                                               // Bank Stmts Path
    "",                                               // App Path
    calcTimeInBusiness(startDate),                    // Time in Business
    toOnDeckDate(startDate),                          // Start Date (OnDeck) MM-DD-YYYY
    toOnDeckDate(dob),                                // DOB (OnDeck) MM-DD-YYYY
    toISODate(dob),                                   // DOB (Headway) YYYY-MM-DD
    data.entity_type_full || "",                      // Entity Type (Credibly)
    entityShort,                                      // Entity Type (PIRS)
  ];
}

function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

/**
 * Build the CSV in mca_uivision format: data row first, then header row.
 */
function buildMCACSV(data) {
  const values = mapToCSVRow(data);
  const dataRow = values.map(csvEscape).join(",");
  const headerRow = CSV_HEADERS.map(csvEscape).join(",");
  return `${dataRow}\n${headerRow}\n`;
}

function writeTempCSV(csvContent) {
  const csvFileName = "mca_uivision.csv";
  const tmpDir = os.tmpdir();
  const filePath = path.join(tmpDir, csvFileName);
  fs.writeFileSync(filePath, csvContent, "utf-8");
  return { filePath, csvFileName };
}

function isChannelAllowed(channelId) {
  if (ALLOWED_CHANNELS.length === 0) return true;
  return ALLOWED_CHANNELS.includes(channelId);
}

// ─── Event Listener ────────────────────────────────────────────────────────────

app.event("message", async ({ event, client, logger }) => {
  try {
    if (event.subtype && event.subtype !== "file_share") return;
    if (!isChannelAllowed(event.channel)) return;
    if (!event.files || event.files.length === 0) return;

    const pdfFiles = event.files.filter(
      (file) =>
        file.mimetype === "application/pdf" ||
        file.name?.toLowerCase().endsWith(".pdf")
    );

    if (pdfFiles.length === 0) return;

    await client.reactions.add({
      channel: event.channel,
      timestamp: event.ts,
      name: "hourglass_flowing_sand",
    });

    for (const file of pdfFiles) {
      let tempFilePath = null;

      try {
        logger.info(`Processing PDF: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`);

        const base64Data = await downloadSlackFile(file.url_private);
        const extractedData = await extractFieldsFromPDF(base64Data, file.name);

        logger.info(`Extracted fields from ${file.name}`);

        // Build CSV in mca_uivision format
        const csvContent = buildMCACSV(extractedData);
        const { filePath, csvFileName } = writeTempCSV(csvContent);
        tempFilePath = filePath;

        // Upload as mca_uivision.csv
        await client.filesUploadV2({
          channel_id: event.channel,
          thread_ts: event.ts,
          file: fs.createReadStream(filePath),
          filename: csvFileName,
          title: csvFileName,
          initial_comment: `📄 Extracted data from \`${file.name}\` → \`mca_uivision.csv\``,
        });

        logger.info(`Uploaded: ${csvFileName}`);
      } catch (fileError) {
        logger.error(`Error processing file ${file.name}:`, fileError);

        await client.chat.postMessage({
          channel: event.channel,
          thread_ts: event.ts,
          text: `⚠️ Couldn't extract data from \`${file.name}\`. Error: ${fileError.message}`,
        });
      } finally {
        if (tempFilePath && fs.existsSync(tempFilePath)) {
          fs.unlinkSync(tempFilePath);
        }
      }
    }

    await client.reactions.remove({
      channel: event.channel,
      timestamp: event.ts,
      name: "hourglass_flowing_sand",
    });
    await client.reactions.add({
      channel: event.channel,
      timestamp: event.ts,
      name: "white_check_mark",
    });
  } catch (error) {
    logger.error("Error handling message event:", error);
  }
});

// ─── Slash Command ─────────────────────────────────────────────────────────────

app.command("/analyze", async ({ command, ack, respond, logger }) => {
  await ack();
  await respond({
    text: "📎 Upload a MobyCap PDF application to this channel and I'll extract it into mca_uivision.csv!",
    response_type: "ephemeral",
  });
});

// ─── Start ─────────────────────────────────────────────────────────────────────

(async () => {
  const port = process.env.PORT || 3000;
  await app.start(port);

  console.log(`⚡ MobyCap PDF → CSV Extractor is running`);
  console.log(`   Model: ${CLAUDE_MODEL}`);
  console.log(`   Prompt caching: enabled (system prompt cached for 5 min)`);
  console.log(`   Output: mca_uivision.csv`);
  console.log(`   Channels: ${ALLOWED_CHANNELS.length > 0 ? ALLOWED_CHANNELS.join(", ") : "All channels"}`);
})();
