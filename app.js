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

// Custom Skill IDs — comma-separated in .env
// Run: curl https://api.anthropic.com/v1/skills?source=custom -H "x-api-key: KEY" -H "anthropic-version: 2023-06-01" -H "anthropic-beta: skills-2025-10-02"
const SKILL_IDS = process.env.SKILL_IDS
  ? process.env.SKILL_IDS.split(",").map((s) => s.trim())
  : [];

// System prompt — cached across all requests to save ~90% on input tokens.
// This prompt is sent once, cached for 5 min, and reused on every subsequent PDF.
const SYSTEM_PROMPT = `You are a document data extractor for a commercial lending company. Analyze PDF applications and extract every field into structured data.

Rules:
- Extract ALL visible fields from the PDF application
- Each key should be the field label as it appears on the form
- Each value should be the corresponding filled-in value
- If a field is empty or not filled in, use an empty string ""
- If there are multiple sections (e.g. Owner 1, Owner 2), prefix keys with the section (e.g. "Owner 1 - Name")
- For checkboxes or yes/no fields, use "Yes" or "No"
- For dates, preserve the format as written
- Flatten everything into a single-level JSON object (no nesting)

Output format: Return ONLY a valid JSON object. No markdown fences, no explanation, no preamble.

Example:
{"Business Name":"Acme Corp","DBA":"Acme","Business Address":"123 Main St","City":"Austin","State":"TX","Zip":"78701","EIN":"12-3456789","Owner 1 - Name":"John Smith","Owner 1 - SSN":"123-45-6789","Requested Amount":"$50,000","Use of Funds":"Working Capital"}`;

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

/**
 * Download a file from Slack using the bot token for auth.
 */
async function downloadSlackFile(url) {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${process.env.SLACK_BOT_TOKEN}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to download file: ${response.status} ${response.statusText}`);
  }

  const buffer = await response.arrayBuffer();
  return Buffer.from(buffer).toString("base64");
}

/**
 * Send a PDF to Claude using Skills + code execution (if configured),
 * or direct prompt with caching (default).
 *
 * The system prompt has cache_control set to "ephemeral" so it gets cached
 * for 5 minutes. Since every PDF uses the same system prompt, the 2nd+ request
 * within 5 min pays ~10% of the input cost for the system prompt tokens.
 */
async function extractFieldsFromPDF(base64Data, fileName) {
  // ── Build the request ──────────────────────────────────────────────────────
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
            text: `Extract all fields from this PDF application.\n\nFile name: ${fileName}`,
          },
        ],
      },
    ],
  };

  // ── If custom Skills are configured, use the Skills + code execution endpoint
  if (SKILL_IDS.length > 0) {
    requestBody.tools = [
      { type: "code_execution_20250825", name: "code_execution" },
    ];
    requestBody.container = {
      skills: SKILL_IDS.map((id) => ({
        type: "custom",
        skill_id: id,
        version: "latest",
      })),
    };

    // Skills require beta headers — use raw HTTP since the SDK
    // may not support all beta features yet
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "code-execution-2025-08-25,skills-2025-10-02",
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`Claude API error ${response.status}: ${errorBody}`);
    }

    let data = await response.json();

    // Handle pause_turn — Skills may need multiple rounds
    while (data.stop_reason === "pause_turn") {
      const continueResponse = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": process.env.ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01",
          "anthropic-beta": "code-execution-2025-08-25,skills-2025-10-02",
        },
        body: JSON.stringify({
          model: CLAUDE_MODEL,
          max_tokens: 4096,
          container: { id: data.container.id },
          system: requestBody.system,
          messages: [
            ...requestBody.messages,
            { role: "assistant", content: data.content },
            { role: "user", content: "Continue processing." },
          ],
          tools: requestBody.tools,
        }),
      });

      if (!continueResponse.ok) {
        const errorBody = await continueResponse.text();
        throw new Error(`Claude API continue error ${continueResponse.status}: ${errorBody}`);
      }

      data = await continueResponse.json();
    }

    return parseClaudeResponse(data.content);
  }

  // ── Standard path (no Skills) — use SDK with prompt caching ────────────────
  const message = await anthropic.messages.create(requestBody);
  return parseClaudeResponse(message.content);
}

/**
 * Parse Claude's response content blocks into a JSON object.
 */
function parseClaudeResponse(content) {
  const rawText = content
    .filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("\n")
    .trim();

  // Strip markdown fences if Claude wraps them
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
 * Escape a value for CSV.
 */
function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

/**
 * Convert a flat JSON object to CSV (header row + one data row).
 */
function jsonToCSV(data) {
  const keys = Object.keys(data);
  const headerRow = keys.map(csvEscape).join(",");
  const dataRow = keys.map((key) => csvEscape(data[key])).join(",");
  return `${headerRow}\n${dataRow}`;
}

/**
 * Write CSV to a temp file.
 */
function writeTempCSV(csvContent, originalFileName) {
  const baseName = path.basename(originalFileName, path.extname(originalFileName));
  const csvFileName = `${baseName}_extracted.csv`;
  const tmpDir = os.tmpdir();
  const filePath = path.join(tmpDir, csvFileName);
  fs.writeFileSync(filePath, csvContent, "utf-8");
  return { filePath, csvFileName };
}

/**
 * Check if the channel is allowed.
 */
function isChannelAllowed(channelId) {
  if (ALLOWED_CHANNELS.length === 0) return true;
  return ALLOWED_CHANNELS.includes(channelId);
}

// ─── Event Listener ────────────────────────────────────────────────────────────

app.event("message", async ({ event, client, logger }) => {
  try {
    // Ignore bot messages, edits, and deletions
    if (event.subtype && event.subtype !== "file_share") return;

    // Check if the channel is allowed
    if (!isChannelAllowed(event.channel)) return;

    // Check if the message has file attachments
    if (!event.files || event.files.length === 0) return;

    // Filter for PDF files only
    const pdfFiles = event.files.filter(
      (file) =>
        file.mimetype === "application/pdf" ||
        file.name?.toLowerCase().endsWith(".pdf")
    );

    if (pdfFiles.length === 0) return;

    // React to show we're processing
    await client.reactions.add({
      channel: event.channel,
      timestamp: event.ts,
      name: "hourglass_flowing_sand",
    });

    // Process each PDF
    for (const file of pdfFiles) {
      let tempFilePath = null;

      try {
        logger.info(`Processing PDF: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`);

        // Download the file from Slack
        const base64Data = await downloadSlackFile(file.url_private);

        // Send to Claude for field extraction
        const extractedData = await extractFieldsFromPDF(base64Data, file.name);
        const fieldCount = Object.keys(extractedData).length;

        logger.info(`Extracted ${fieldCount} fields from ${file.name}`);

        // Convert to CSV
        const csvContent = jsonToCSV(extractedData);
        const { filePath, csvFileName } = writeTempCSV(csvContent, file.name);
        tempFilePath = filePath;

        // Upload the CSV to Slack
        await client.filesUploadV2({
          channel_id: event.channel,
          thread_ts: event.ts,
          file: fs.createReadStream(filePath),
          filename: csvFileName,
          title: csvFileName,
          initial_comment: `📄 Extracted *${fieldCount} fields* from \`${file.name}\``,
        });

        logger.info(`Uploaded CSV: ${csvFileName}`);
      } catch (fileError) {
        logger.error(`Error processing file ${file.name}:`, fileError);

        await client.chat.postMessage({
          channel: event.channel,
          thread_ts: event.ts,
          text: `⚠️ Couldn't extract data from \`${file.name}\`. Error: ${fileError.message}`,
        });
      } finally {
        // Clean up temp file
        if (tempFilePath && fs.existsSync(tempFilePath)) {
          fs.unlinkSync(tempFilePath);
        }
      }
    }

    // Remove the hourglass, add checkmark
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
    text: "📎 Upload a PDF application to this channel and I'll extract the fields into a CSV for you!",
    response_type: "ephemeral",
  });
});

// ─── Start ─────────────────────────────────────────────────────────────────────

(async () => {
  const port = process.env.PORT || 3000;
  await app.start(port);

  console.log(`⚡ Slack Claude PDF → CSV Extractor is running`);
  console.log(`   Model: ${CLAUDE_MODEL}`);
  console.log(`   Prompt caching: enabled (system prompt cached for 5 min)`);
  console.log(`   Skills: ${SKILL_IDS.length > 0 ? SKILL_IDS.join(", ") : "None (using direct prompt)"}`);
  console.log(`   Channels: ${ALLOWED_CHANNELS.length > 0 ? ALLOWED_CHANNELS.join(", ") : "All channels"}`);
})();
