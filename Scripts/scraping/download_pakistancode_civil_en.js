/**
 * download_pakistancode_civil_en.js
 * - Downloads Civil Laws PDFs (Pakistan Code, English version)
 * - Saves into Data/raw/pakistancode/civil_en/
 * - Maintains Data/metadata/pakistancode_civil_manifest_en.json
 * - Logs errors into Data/metadata/download_errors_en.log
 */

const puppeteer = require('puppeteer');
const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const sanitize = require('sanitize-filename');
const pLimit = require('p-limit').default;
const crypto = require('crypto');

const START_URL = 'https://pakistancode.gov.pk/english/LGu0xVD-apaUY2Fqa-aw%3D%3D&action=primary&catid=2';
const BASE = 'https://pakistancode.gov.pk';

const OUT_DIR = path.resolve(__dirname, '..', '..', 'Data', 'raw', 'pakistancode_civil_pdfs_en');
const META_DIR = path.resolve(__dirname, '..', '..', 'Data', 'metadata');
const MANIFEST_PATH = path.join(META_DIR, 'pakistancode_civil_manifest_en.json');
const ERROR_LOG = path.join(META_DIR, 'download_errors_en.log');

const CONCURRENCY = 3;


function now12Hour() {
  const d = new Date();
  let hours = d.getHours();
  const minutes = d.getMinutes().toString().padStart(2, '0');
  const seconds = d.getSeconds().toString().padStart(2, '0');
  const ampm = hours >= 12 ? 'PM' : 'AM';
  hours = hours % 12;
  hours = hours ? hours : 12;
  const month = (d.getMonth() + 1).toString().padStart(2, '0');
  const day = d.getDate().toString().padStart(2, '0');
  const year = d.getFullYear();
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} ${ampm}`;
}

async function appendErrorLog(entry) {
  const line = `[${now12Hour()}] ${entry}\n`;
  await fs.appendFile(ERROR_LOG, line, 'utf8');
}

async function sha256File(filePath) {
  return new Promise((res, rej) => {
    const hash = crypto.createHash('sha256');
    const rs = fs.createReadStream(filePath);
    rs.on('error', rej);
    rs.on('data', chunk => hash.update(chunk));
    rs.on('end', () => res(hash.digest('hex')));
  });
}

async function downloadStream(url, destPath) {
  const resp = await axios.get(url, {
    responseType: 'stream',
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SmartLawyerBot/1.0)' },
    timeout: 120000
  });
  await fs.ensureDir(path.dirname(destPath));
  const writer = fs.createWriteStream(destPath);
  resp.data.pipe(writer);
  await new Promise((resolve, reject) => {
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
  const stat = await fs.stat(destPath);
  const sha = await sha256File(destPath);
  return { size: stat.size, sha256: sha };
}

async function main() {
  await fs.ensureDir(OUT_DIR);
  await fs.ensureDir(META_DIR);

  let manifest = {};
  if (await fs.pathExists(MANIFEST_PATH)) {
    try {
      manifest = await fs.readJson(MANIFEST_PATH);
    } catch (e) {
      manifest = {};
    }
  }

  const browser = await puppeteer.launch({ headless: false, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  await page.goto(START_URL, { waitUntil: 'networkidle2', timeout: 60000 });
  // ...after await page.goto(START_URL, ...);
// await page.screenshot({ path: 'debug_page.png', fullPage: true }); // Debug screenshot
  await page.waitForSelector('a', { timeout: 15000 }); // Wait for at least one <a> tag

  // Get all law page links (relative hrefs, likely law pages)
  const lawPages = await page.$$eval('a', as =>
    as
      .filter(a => {
        const href = a.getAttribute('href');
        return (
          href &&
          !href.startsWith('http') &&
          !href.startsWith('/') &&
          href.startsWith('UY2FqaJw1-apaUY2Fqa')
        );
      })
      .map(a => {
        const rawHref = a.getAttribute('href');
        const fullHref = rawHref.startsWith('/english/')
          ? rawHref
          : '/english/' + rawHref.replace(/^\/+/, '');
        return {
          href: new URL(fullHref, window.location.origin).href,
          text: a.textContent.trim()
        };
      })
      .filter(x => x.text && x.text.length > 5)
  );

  // ...existing code...
// Debug code commented out
// const allLinks = await page.$$eval('a', as =>
//   as.map(a => ({
//     href: a.getAttribute('href'),
//     text: a.textContent.trim()
//   }))
// );
// console.log('All <a> links:', allLinks);
// ...existing code...

  const unique = Array.from(new Map(lawPages.map(l => [l.href, l])).values());
  console.log(`Found ${unique.length} candidate law pages.`);

  const limit = pLimit(CONCURRENCY);

  const tasks = unique.map(item =>
    limit(async () => {
      const key = item.href;
      try {
        if (item.href.includes('action=primary') || item.href.endsWith('/english/')) return;

        if (manifest[key] && manifest[key].status === 'downloaded' && await fs.pathExists(manifest[key].local_path)) {
          console.log('[skip]', item.text);
          return;
        }

        const lawPage = await browser.newPage();
        await lawPage.goto(item.href, { waitUntil: 'networkidle2', timeout: 60000 });

        let pdfHref = await lawPage.$$eval('a', as => {
          const candidates = as.map(a => a.href).filter(h => h && (h.toLowerCase().includes('/pdffiles/') || h.toLowerCase().endsWith('.pdf')));
          return candidates.length ? candidates[0] : null;
        });

        if (!pdfHref) {
          console.warn('[no-pdf]', item.href);
          manifest[key] = { title: item.text, law_page: item.href, status: 'no-pdf-found', checked_at: await nowISO() };
          await lawPage.close();
          return;
        }

        const resolvedPdfUrl = pdfHref.startsWith('http') ? pdfHref : new URL(pdfHref, BASE).href;
        // Remove address/hash from filename, only use sanitized title + .pdf
        const cleanName = sanitize(item.text).slice(0, 180) + '.pdf';
        const outFile = path.join(OUT_DIR, cleanName);

        console.log('[download]', item.text);
        let downloadedMeta;
        try {
          downloadedMeta = await downloadStream(resolvedPdfUrl, outFile);
        } catch (err) {
          const errMsg = `Download failed: ${resolvedPdfUrl} | error: ${err.message}`;
          console.error('[download-error]', errMsg);
          await appendErrorLog(errMsg);
          manifest[key] = { title: item.text, law_page: item.href, pdf_url: resolvedPdfUrl, status: 'download-error', error: err.message };
          await lawPage.close();
          return;
        }

        // Store local_path as relative to project root
        const relOutFile = path.relative(path.resolve(__dirname, '..', '..'), outFile).replace(/\\/g, '/');

        manifest[key] = {
          title: item.text,
          law_page: item.href,
          pdf_url: resolvedPdfUrl,
          local_path: relOutFile,
          size: downloadedMeta.size,
          sha256: downloadedMeta.sha256,
          status: 'downloaded',
          downloaded_at: now12Hour()
        };

        await lawPage.close();
        await fs.writeJson(MANIFEST_PATH, manifest, { spaces: 2 });
      } catch (err) {
  const errMsg = `General error: ${item.href} | error: ${err.message}`;
  console.error('[err]', errMsg);
  await appendErrorLog(errMsg);
  manifest[key] = { title: item.text, law_page: item.href, status: 'error', error: err.message, checked_at: now12Hour() };
  await fs.writeJson(MANIFEST_PATH, manifest, { spaces: 2 });
      }
    })
  );

  await Promise.all(tasks);
  await browser.close();
  await fs.writeJson(MANIFEST_PATH, manifest, { spaces: 2 });
  console.log('✅ Done. Manifest saved to', MANIFEST_PATH);
}

main().catch(async err => {
  console.error('Fatal script error:', err.message);
  await appendErrorLog(`Fatal script error: ${err.message}`);
  process.exit(1);
});
