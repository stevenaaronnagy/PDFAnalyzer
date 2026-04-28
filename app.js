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

// Salesforce config
const SF_DOMAIN = process.env.SF_DOMAIN || "";
const SF_CLIENT_ID = process.env.SF_CLIENT_ID || "";
const SF_CLIENT_SECRET = process.env.SF_CLIENT_SECRET || "";
const MERCHANT_RECORD_TYPE_ID = process.env.SF_MERCHANT_RECORD_TYPE_ID || "0126g000000cifIAAQ";
const CREDIT_APP_RECORD_TYPE_ID = process.env.SF_CREDIT_APP_RECORD_TYPE_ID || "012Rn00000131QPIAY";
const FUNDING_EXECUTIVE_ID = process.env.SF_FUNDING_EXECUTIVE_ID || "005Rn0000058R1nIAE";

// In-memory store: maps thread_ts → extracted data from PDF
const threadDataStore = new Map();

// ─── CSV Column Definitions ────────────────────────────────────────────────────
const CSV_HEADERS = [
  "Legal Name", "DBA", "Tax ID", "Entity Type", "Start Date", "Industry",
  "Business Phone", "Address", "City", "State", "ZIP", "Annual Revenue",
  "Monthly Revenue", "Avg Bank Balance", "Avg Daily Ledger", "Monthly CC Volume",
  "Bankruptcies", "Judgments", "Tax Liens", "Open MCA", "Federal Contract",
  "Owner First", "Owner Last", "SSN", "DOB", "Email", "Home Phone", "Cell Phone",
  "Owner Address", "Owner City", "Owner State", "Owner ZIP", "Ownership %",
  "Amount Requested", "Purpose", "Bank Stmts Path", "App Path", "Time in Business",
  "Start Date (OnDeck)", "DOB (OnDeck)", "DOB (Headway)", "Entity Type (Credibly)",
  "Entity Type (PIRS)",
];

// ─── System Prompt ─────────────────────────────────────────────────────────────
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

// ─── Salesforce Client ─────────────────────────────────────────────────────────

class SalesforceClient {
  constructor() {
    this.instanceUrl = null;
    this.accessToken = null;
  }

  async authenticate() {
    const response = await fetch(`https://${SF_DOMAIN}/services/oauth2/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: SF_CLIENT_ID,
        client_secret: SF_CLIENT_SECRET,
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      throw new Error(`SF auth failed: ${response.status} ${errText}`);
    }

    const data = await response.json();
    this.instanceUrl = data.instance_url;
    this.accessToken = data.access_token;
  }

  headers() {
    return {
      Authorization: `Bearer ${this.accessToken}`,
      "Content-Type": "application/json",
    };
  }

  async query(soql) {
    const url = `${this.instanceUrl}/services/data/v59.0/query?q=${encodeURIComponent(soql)}`;
    const r = await fetch(url, { headers: this.headers() });
    if (!r.ok) {
      const errText = await r.text();
      throw new Error(`SF query failed: ${r.status} ${errText}`);
    }
    return r.json();
  }

  async create(objectName, fields) {
    const url = `${this.instanceUrl}/services/data/v59.0/sobjects/${objectName}`;
    const r = await fetch(url, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify(fields),
    });
    if (r.status === 201) {
      const data = await r.json();
      return data.id;
    }
    const errText = await r.text();
    throw new Error(`SF create ${objectName} failed: ${r.status} ${errText}`);
  }

  async update(objectName, recordId, fields) {
    const url = `${this.instanceUrl}/services/data/v59.0/sobjects/${objectName}/${recordId}`;
    const r = await fetch(url, {
      method: "PATCH",
      headers: this.headers(),
      body: JSON.stringify(fields),
    });
    if (r.status === 204) return recordId;
    const errText = await r.text();
    throw new Error(`SF update ${objectName} failed: ${r.status} ${errText}`);
  }
}

/**
 * Find Account by EIN or Business Name, or create a new one.
 */
async function findOrCreateAccount(sf, data) {
  const ein = (data.tax_id || "").replace(/-/g, "").trim();
  const legalName = data.legal_name || "";

  // Try finding by EIN
  if (ein) {
    const result = await sf.query(
      `SELECT Id FROM Account WHERE McaApp__Federal_Tax_ID_No__c='${ein}' LIMIT 1`
    );
    if (result.totalSize > 0) {
      return { id: result.records[0].Id, method: "ein_match" };
    }
  }

  // Try finding by name
  if (legalName) {
    const escapedName = legalName.replace(/'/g, "\\'");
    const result = await sf.query(
      `SELECT Id FROM Account WHERE Name='${escapedName}' AND RecordTypeId='${MERCHANT_RECORD_TYPE_ID}' LIMIT 1`
    );
    if (result.totalSize > 0) {
      return { id: result.records[0].Id, method: "name_match" };
    }
  }

  // Create new Account
  const accountFields = {};
  if (legalName) accountFields.Name = legalName;
  else accountFields.Name = "Unknown";
  accountFields.RecordTypeId = MERCHANT_RECORD_TYPE_ID;
  if (ein) accountFields.McaApp__Federal_Tax_ID_No__c = ein;
  if (data.business_phone) accountFields.Phone = data.business_phone;
  accountFields.Type = "Prospect";

  const accountId = await sf.create("Account", accountFields);
  return { id: accountId, method: "created" };
}

/**
 * Find existing Credit Application by EIN + Business Name, or return null.
 */
async function findCreditApp(sf, data) {
  const ein = (data.tax_id || "").replace(/-/g, "").trim();
  const legalName = data.legal_name || "";

  if (ein) {
    const result = await sf.query(
      `SELECT Id FROM cloudmaveninc__Credit_Application__c WHERE Federal_Tax_ID__c='${ein}' ORDER BY CreatedDate DESC LIMIT 1`
    );
    if (result.totalSize > 0) return result.records[0].Id;
  }

  if (legalName) {
    const escapedName = legalName.replace(/'/g, "\\'");
    const result = await sf.query(
      `SELECT Id FROM cloudmaveninc__Credit_Application__c WHERE cloudmaveninc__Business_Name_DBA__c='${escapedName}' ORDER BY CreatedDate DESC LIMIT 1`
    );
    if (result.totalSize > 0) return result.records[0].Id;
  }

  return null;
}

/**
 * Build Salesforce Credit Application fields from extracted data.
 */
function buildCreditAppFields(data, accountId) {
  const annualRev = parseRevenue(data.annual_revenue);
  const monthlyRev = data.monthly_revenue ? parseRevenue(data.monthly_revenue) : (annualRev / 12);
  const amountRequested = parseRevenue(data.amount_requested);
  const ownershipPct = parseFloat((data.ownership_pct || "").replace(/[^0-9.]/g, "")) || null;
  const ssn = (data.owner_ssn || "").replace(/-/g, "").trim();
  const bizState = toStateAbbrev(data.state);
  const ownerState = toStateAbbrev(data.owner_state);
  const bizStateFull = toFullStateName(data.state);
  const ownerStateFull = toFullStateName(data.owner_state);

  const fields = {
    cloudmaveninc__Account__c: accountId,
    RecordTypeId: CREDIT_APP_RECORD_TYPE_ID,
    Funding_Executive__c: FUNDING_EXECUTIVE_ID,
    OwnerId: FUNDING_EXECUTIVE_ID,

    // Business info
    cloudmaveninc__Business_Legal_Name__c: data.legal_name || null,
    cloudmaveninc__Business_Name_DBA__c: data.dba || data.legal_name || null,
    Federal_Tax_ID__c: data.tax_id || null,
    cloudmaveninc__Business_Entity_Type__c: data.entity_type_full || null,
    cloudmaveninc__Business_Inception_New__c: toISODate(data.start_date) || null,
    cloudmaveninc__Business_Industry__c: data.industry || null,
    cloudmaveninc__Business_Phone__c: data.business_phone || null,
    cloudmaveninc__Business_Physical_Street__c: data.address || null,
    cloudmaveninc__Business_Physical_City__c: data.city || null,
    cloudmaveninc__Business_Physical_State_Province__c: bizStateFull || null,
    Business_State__c: bizState || null,
    cloudmaveninc__Business_Physical_Postal__c: data.zip || null,
    cloudmaveninc__Business_Country_Location__c: "United States",
    cloudmaveninc__Business_Mailing_Country__c: "United States",

    // Financials
    cloudmaveninc__Gross_Annual_Income__c: annualRev || null,
    cloudmaveninc__Monthly_Income_before_taxes__c: monthlyRev || null,
    cloudmaveninc__Amount_Requested__c: amountRequested || null,
    cloudmaveninc__Reason_for_Finance_Request__c: data.purpose || null,
    cloudmaveninc__Credit_Score__c: data.estimated_credit_score ? parseInt(data.estimated_credit_score) : null,
    Current_Advances_and_Balances__c: data.current_advances || null,

    // Primary owner
    cloudmaveninc__Applicant_First_Name__c: data.owner_first || null,
    cloudmaveninc__Applicant_Last_Name__c: data.owner_last || null,
    cloudmaveninc__Applicant_SSN_SIN__c: ssn || null,
    cloudmaveninc__Applicant_Date_of_Birth__c: toISODate(data.owner_dob) || null,
    cloudmaveninc__Applicant_Email__c: data.owner_email || null,
    cloudmaveninc__Applicant_Phone__c: data.owner_phone || null,
    cloudmaveninc__Applicant_Mobile_Phone__c: data.owner_phone || null,
    cloudmaveninc__Applicant_Physical_Street__c: data.owner_address || null,
    cloudmaveninc__Applicant_Physical_City__c: data.owner_city || null,
    cloudmaveninc__Applicant_Physical_State_Province__c: ownerStateFull || null,
    cloudmaveninc__Applicant_Physical_Postal__c: data.owner_zip || null,
    cloudmaveninc__Applicant_of_Ownership__c: ownershipPct,

    // 2nd owner (co-applicant)
    Co_Applicant_1_Name__c: (data.owner2_first && data.owner2_last)
      ? `${data.owner2_first} ${data.owner2_last}` : null,
    Co_Applicant_1_SSN__c: data.owner2_ssn ? data.owner2_ssn.replace(/-/g, "") : null,
    Co_Applicant_1_DOB__c: toISODate(data.owner2_dob) || null,
    Co_Applicant_1_Email__c: data.owner2_email || null,
    Co_Applicant_1_Ownership__c: data.owner2_ownership_pct ? parseFloat(data.owner2_ownership_pct) : null,
    Co_App_1_Physical_Street__c: data.owner2_address || null,
    Co_App_1_Physical_City__c: data.owner2_city || null,
    Co_App_1_Physical_State__c: data.owner2_state ? toFullStateName(data.owner2_state) : null,
    Co_App_1_Postal_Code__c: data.owner2_zip || null,

    // Defaults
    cloudmaveninc__Application_Type__c: "Business",
    cloudmaveninc__Immigration_Status__c: "US Citizens Only",
    cloudmaveninc__Credit_Application_Status__c: "Draft",
  };

  // Remove null values — Salesforce doesn't like them on create
  const cleaned = {};
  for (const [key, value] of Object.entries(fields)) {
    if (value !== null && value !== undefined && value !== "") {
      cleaned[key] = value;
    }
  }

  return cleaned;
}

/**
 * Main Salesforce upsert: find/create Account, find/create Credit App.
 */
async function upsertToSalesforce(data) {
  if (!SF_DOMAIN || !SF_CLIENT_ID || !SF_CLIENT_SECRET) {
    throw new Error("Salesforce credentials not configured. Add SF_DOMAIN, SF_CLIENT_ID, SF_CLIENT_SECRET to .env");
  }

  const sf = new SalesforceClient();
  await sf.authenticate();

  // Find or create Account
  const account = await findOrCreateAccount(sf, data);

  // Build Credit App fields
  const caFields = buildCreditAppFields(data, account.id);

  // Find existing Credit App or create new
  const existingCaId = await findCreditApp(sf, data);

  let caId, action;
  if (existingCaId) {
    // Update existing — remove RecordTypeId and OwnerId to avoid permission issues
    const updateFields = { ...caFields };
    delete updateFields.RecordTypeId;
    delete updateFields.OwnerId;
    await sf.update("cloudmaveninc__Credit_Application__c", existingCaId, updateFields);
    caId = existingCaId;
    action = "updated";
  } else {
    caId = await sf.create("cloudmaveninc__Credit_Application__c", caFields);
    action = "created";
  }

  return {
    accountId: account.id,
    accountMethod: account.method,
    creditAppId: caId,
    creditAppAction: action,
  };
}

// ─── PDF / CSV Helpers ─────────────────────────────────────────────────────────

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

function toOnDeckDate(dateStr) {
  if (!dateStr) return "";
  return dateStr.replace(/\//g, "-");
}

function parseRevenue(str) {
  if (!str) return 0;
  return parseFloat(str.replace(/[$,]/g, "")) || 0;
}

function formatWithCommas(num) {
  if (!num && num !== 0) return "";
  const parts = num.toFixed(2).split(".");
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  if (parts[1] === "00") return parts[0];
  return parts.join(".");
}

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
  if (state.length === 2 && state === state.toUpperCase()) return state;
  const abbrev = STATE_ABBREVS[state.toLowerCase().trim()];
  return abbrev || state;
}

/**
 * Convert 2-letter state abbreviation to full name (for SF picklist fields).
 */
const ABBREV_TO_STATE = Object.fromEntries(
  Object.entries(STATE_ABBREVS).map(([name, abbr]) => [abbr, name.replace(/\b\w/g, c => c.toUpperCase())])
);

function toFullStateName(state) {
  if (!state) return "";
  // Already a full name
  if (state.length > 2) return state;
  return ABBREV_TO_STATE[state.toUpperCase()] || state;
}

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

function mapToCSVRow(data) {
  const entityShort = data.entity_type_short || getEntityTypeShort(data.entity_type_full);
  const startDate = data.start_date || "";
  const dob = data.owner_dob || "";
  const legalName = data.legal_name || "";
  const dba = data.dba || legalName;
  const annualRev = parseRevenue(data.annual_revenue);
  const monthlyRev = data.monthly_revenue ? parseRevenue(data.monthly_revenue) : (annualRev / 12);
  const avgDailyLedger = monthlyRev > 0 ? (monthlyRev / 30) : 0;
  const bizState = toStateAbbrev(data.state);
  const ownerState = toStateAbbrev(data.owner_state);

  return [
    legalName, dba, data.tax_id || "", entityShort, startDate,
    data.industry || "", data.business_phone || "", data.address || "",
    data.city || "", bizState, data.zip || "",
    annualRev ? formatWithCommas(annualRev) : "",
    monthlyRev ? formatWithCommas(monthlyRev) : "",
    monthlyRev ? formatWithCommas(monthlyRev) : "",
    avgDailyLedger ? formatWithCommas(avgDailyLedger) : "",
    "", "Yes", "Yes", "No", "No", "Yes",
    data.owner_first || "", data.owner_last || "", data.owner_ssn || "",
    dob, data.owner_email || "", data.owner_phone || "", data.owner_phone || "",
    data.owner_address || "", data.owner_city || "", ownerState,
    data.owner_zip || "", data.ownership_pct || "",
    data.amount_requested || "", data.purpose || "",
    "", "",
    calcTimeInBusiness(startDate),
    toOnDeckDate(startDate), toOnDeckDate(dob), toISODate(dob),
    data.entity_type_full || "", entityShort,
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

// ─── Event Listener: PDF Upload ────────────────────────────────────────────────

app.event("message", async ({ event, client, logger }) => {
  try {
    // ── Handle "create" keyword in threads ───────────────────────────────────
    if (
      event.thread_ts &&
      !event.subtype &&
      event.text &&
      event.text.trim().toLowerCase() === "create"
    ) {
      await handleCreateCommand(event, client, logger);
      return;
    }

    // ── Handle PDF uploads ───────────────────────────────────────────────────
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

        // Store extracted data keyed by thread timestamp for later "create" command
        const threadTs = event.ts;
        threadDataStore.set(threadTs, {
          data: extractedData,
          fileName: file.name,
          timestamp: Date.now(),
        });

        // Clean up old entries (older than 24 hours)
        const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
        for (const [key, val] of threadDataStore) {
          if (val.timestamp < oneDayAgo) threadDataStore.delete(key);
        }

        // Build and upload CSV
        const csvContent = buildMCACSV(extractedData);
        const { filePath, csvFileName } = writeTempCSV(csvContent);
        tempFilePath = filePath;

        await client.filesUploadV2({
          channel_id: event.channel,
          thread_ts: event.ts,
          file: fs.createReadStream(filePath),
          filename: csvFileName,
          title: csvFileName,
          initial_comment: `📄 Extracted data from \`${file.name}\` → \`mca_uivision.csv\`\n\n💡 Reply *create* in this thread to create/update the Salesforce record.`,
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

// ─── "create" Command Handler ──────────────────────────────────────────────────

async function handleCreateCommand(event, client, logger) {
  const threadTs = event.thread_ts;

  // Look up the extracted data for this thread
  const stored = threadDataStore.get(threadTs);
  if (!stored) {
    await client.chat.postMessage({
      channel: event.channel,
      thread_ts: threadTs,
      text: "⚠️ No extracted data found for this thread. The data may have expired (24hr limit) or the PDF wasn't processed in this thread.",
    });
    return;
  }

  // React to show we're processing
  await client.reactions.add({
    channel: event.channel,
    timestamp: event.ts,
    name: "hourglass_flowing_sand",
  });

  try {
    logger.info(`Salesforce upsert triggered for: ${stored.fileName}`);

    const result = await upsertToSalesforce(stored.data);

    const sfUrl = `${SF_DOMAIN.replace(".my.salesforce.com", "")}.lightning.force.com`;

    await client.chat.postMessage({
      channel: event.channel,
      thread_ts: threadTs,
      text: [
        `✅ *Salesforce record ${result.creditAppAction}!*`,
        ``,
        `• *Account:* ${result.accountMethod === "created" ? "Created new" : `Found existing (${result.accountMethod})`}`,
        `• *Credit Application:* ${result.creditAppAction === "created" ? "Created new" : "Updated existing"}`,
        `• *Business:* ${stored.data.legal_name || "Unknown"}`,
        `• *Amount:* ${stored.data.amount_requested || "N/A"}`,
        ``,
        `🔗 <https://${sfUrl}/lightning/r/cloudmaveninc__Credit_Application__c/${result.creditAppId}/view|View in Salesforce>`,
      ].join("\n"),
    });

    logger.info(`SF upsert complete | Account: ${result.accountId} (${result.accountMethod}) | Credit App: ${result.creditAppId} (${result.creditAppAction})`);
  } catch (sfError) {
    logger.error("Salesforce upsert failed:", sfError);

    await client.chat.postMessage({
      channel: event.channel,
      thread_ts: threadTs,
      text: `⚠️ Salesforce error: ${sfError.message}`,
    });
  }

  // Remove hourglass
  try {
    await client.reactions.remove({
      channel: event.channel,
      timestamp: event.ts,
      name: "hourglass_flowing_sand",
    });
  } catch {
    // Ignore if reaction wasn't added
  }
}

// ─── Slash Command ─────────────────────────────────────────────────────────────

app.command("/analyze", async ({ command, ack, respond, logger }) => {
  await ack();
  await respond({
    text: "📎 Upload a MobyCap PDF application to this channel and I'll extract it into mca_uivision.csv!\n\nThen reply *create* in the thread to push it to Salesforce.",
    response_type: "ephemeral",
  });
});

// ─── Start ─────────────────────────────────────────────────────────────────────

(async () => {
  const port = process.env.PORT || 3000;
  await app.start(port);

  console.log(`⚡ MobyCap PDF → CSV + Salesforce Extractor is running`);
  console.log(`   Model: ${CLAUDE_MODEL}`);
  console.log(`   Prompt caching: enabled (system prompt cached for 5 min)`);
  console.log(`   Output: mca_uivision.csv`);
  console.log(`   Salesforce: ${SF_DOMAIN ? "configured" : "NOT configured (add SF_DOMAIN, SF_CLIENT_ID, SF_CLIENT_SECRET to .env)"}`);
  console.log(`   Channels: ${ALLOWED_CHANNELS.length > 0 ? ALLOWED_CHANNELS.join(", ") : "All channels"}`);
})();
