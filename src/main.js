// Vitals.com Physician Scraper - Production Ready, Fast & Stealthy
// Hybrid approach: Playwright Firefox for listings + got-scraping/Cheerio for details
import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset, KeyValueStore } from 'crawlee';
import { firefox } from 'playwright';
import { gotScraping } from 'got-scraping';
import { load as cheerioLoad } from 'cheerio';

const BASE_URL = 'https://www.vitals.com';
const SESSION_KEY = 'VITALS_SESSION';

// ============================================================================
// SPECIALTY & LOCATION MAPPINGS
// ============================================================================

const SPECIALTY_SLUGS = {
    'cardiovascular disease': 'cardiologists',
    'cardiology': 'cardiologists',
    'cardiologist': 'cardiologists',
    'dermatology': 'dermatologists',
    'dermatologist': 'dermatologists',
    'family medicine': 'family-medicine-doctors',
    'family practice': 'family-medicine-doctors',
    'internal medicine': 'internists',
    'internist': 'internists',
    'orthopedic surgery': 'orthopedic-surgeons',
    'orthopedics': 'orthopedic-surgeons',
    'pediatrics': 'pediatricians',
    'pediatrician': 'pediatricians',
    'psychiatry': 'psychiatrists',
    'psychiatrist': 'psychiatrists',
    'neurology': 'neurologists',
    'neurologist': 'neurologists',
    'obstetrics gynecology': 'obstetricians-gynecologists',
    'ob-gyn': 'obstetricians-gynecologists',
    'ophthalmology': 'ophthalmologists',
    'ophthalmologist': 'ophthalmologists',
    'dentist': 'dentists',
    'dentistry': 'dentists',
    'gastroenterology': 'gastroenterologists',
    'gastroenterologist': 'gastroenterologists',
    'urology': 'urologists',
    'urologist': 'urologists',
    'pulmonology': 'pulmonologists',
    'pulmonologist': 'pulmonologists',
    'endocrinology': 'endocrinologists',
    'endocrinologist': 'endocrinologists',
    'rheumatology': 'rheumatologists',
    'rheumatologist': 'rheumatologists',
    'oncology': 'oncologists',
    'oncologist': 'oncologists',
    'allergy immunology': 'allergists-immunologists',
    'allergist': 'allergists-immunologists',
    'allergy-immunology': 'allergists-immunologists',
    'pain management': 'pain-management-specialists',
    'physical therapy': 'physical-therapists',
    'chiropractor': 'chiropractors',
    'podiatrist': 'podiatrists',
    'optometrist': 'optometrists',
};

const STATE_ABBREVIATIONS = {
    'alabama': 'al', 'alaska': 'ak', 'arizona': 'az', 'arkansas': 'ar', 'california': 'ca',
    'colorado': 'co', 'connecticut': 'ct', 'delaware': 'de', 'florida': 'fl', 'georgia': 'ga',
    'hawaii': 'hi', 'idaho': 'id', 'illinois': 'il', 'indiana': 'in', 'iowa': 'ia',
    'kansas': 'ks', 'kentucky': 'ky', 'louisiana': 'la', 'maine': 'me', 'maryland': 'md',
    'massachusetts': 'ma', 'michigan': 'mi', 'minnesota': 'mn', 'mississippi': 'ms', 'missouri': 'mo',
    'montana': 'mt', 'nebraska': 'ne', 'nevada': 'nv', 'new hampshire': 'nh', 'new jersey': 'nj',
    'new mexico': 'nm', 'new york': 'ny', 'north carolina': 'nc', 'north dakota': 'nd', 'ohio': 'oh',
    'oklahoma': 'ok', 'oregon': 'or', 'pennsylvania': 'pa', 'rhode island': 'ri', 'south carolina': 'sc',
    'south dakota': 'sd', 'tennessee': 'tn', 'texas': 'tx', 'utah': 'ut', 'vermont': 'vt',
    'virginia': 'va', 'washington': 'wa', 'west virginia': 'wv', 'wisconsin': 'wi', 'wyoming': 'wy',
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

const getSpecialtySlug = (specialty) => {
    if (!specialty) return 'doctors';
    const normalized = specialty.toLowerCase().trim().replace(/-/g, ' ');
    return SPECIALTY_SLUGS[normalized] || specialty.toLowerCase().trim().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
};

const parseLocation = (location) => {
    if (!location) return null;
    const normalized = location.toLowerCase().trim();

    if (normalized.includes(',')) {
        const [cityPart, statePart] = normalized.split(',').map(s => s.trim());
        if (cityPart && statePart) {
            const stateAbbrev = statePart.length === 2 ? statePart : STATE_ABBREVIATIONS[statePart];
            if (stateAbbrev) {
                const city = cityPart.replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
                return { state: stateAbbrev, city: city || null };
            }
        }
    }

    const parts = normalized.split(/\s+/);
    if (parts.length >= 2) {
        const lastPart = parts[parts.length - 1];
        const stateAbbrev = lastPart.length === 2 ? lastPart : STATE_ABBREVIATIONS[lastPart];
        if (stateAbbrev) {
            const city = parts.slice(0, -1).join('-').replace(/[^a-z0-9-]/g, '');
            return { state: stateAbbrev, city: city || null };
        }
    }

    if (STATE_ABBREVIATIONS[normalized]) return { state: STATE_ABBREVIATIONS[normalized], city: null };
    if (normalized.length === 2 && Object.values(STATE_ABBREVIATIONS).includes(normalized)) {
        return { state: normalized, city: null };
    }
    return null;
};

const buildListingUrl = ({ specialty, location, page = 1 }) => {
    const specialtySlug = getSpecialtySlug(specialty);
    const locationInfo = parseLocation(location);
    const validCity = locationInfo?.city && locationInfo.city.replace(/-/g, '').length > 0;

    let url;
    if (locationInfo?.state && validCity) {
        url = `${BASE_URL}/${specialtySlug}/${locationInfo.state}/${locationInfo.city}`;
    } else if (locationInfo?.state) {
        url = `${BASE_URL}/${specialtySlug}/${locationInfo.state}`;
    } else {
        url = `${BASE_URL}/${specialtySlug}`;
    }

    if (page > 1) url += `?page=${page}`;
    return url;
};

const cleanText = (text) => {
    if (!text) return null;
    return text.replace(/\s+/g, ' ').trim() || null;
};

const randomDelay = (min = 100, max = 300) =>
    new Promise(resolve => setTimeout(resolve, min + Math.random() * (max - min)));

// ============================================================================
// DATA EXTRACTION FUNCTIONS
// ============================================================================

const extractDoctorsFromListing = ($) => {
    const doctors = [];
    const seenUrls = new Set();

    // Find all doctor profile links
    $('a[href*="/doctors/"]').each((_, el) => {
        try {
            const $el = $(el);
            const href = $el.attr('href');
            if (!href || !href.match(/\/doctors\/[Dd]r-/) || seenUrls.has(href)) return;

            seenUrls.add(href);
            const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;

            // Get name from link or parent
            const $parent = $el.closest('div, article, section, li');
            let name = $el.text().trim();
            if (!name || name.length < 3) {
                name = $parent.find('h2, h3, h4, [class*="name"]').first().text().trim();
            }

            // Skip non-name links
            if (!name || name.length < 3 || name.includes('View') || name.includes('More') || name.includes('See')) return;

            const specialty = $parent.find('[class*="specialty"]').text().trim();
            const location = $parent.find('[class*="location"], [class*="address"]').text().trim();
            const ratingText = $parent.find('[class*="rating"]').text();
            const ratingMatch = ratingText.match(/(\d+\.?\d*)/);

            doctors.push({
                url: fullUrl,
                name: cleanText(name),
                specialty: cleanText(specialty) || null,
                location: cleanText(location) || null,
                rating: ratingMatch ? parseFloat(ratingMatch[1]) : null,
            });
        } catch (err) {
            log.debug(`Error extracting doctor: ${err.message}`);
        }
    });

    return doctors;
};

const extractJsonLd = ($) => {
    let data = null;
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const json = JSON.parse($(el).html());
            const items = Array.isArray(json) ? json : [json];
            for (const item of items) {
                if (['MedicalBusiness', 'Physician', 'Person', 'LocalBusiness'].includes(item['@type'])) {
                    data = {
                        name: item.name || null,
                        specialty: item.medicalSpecialty?.name || item.specialty || null,
                        bio: item.description || null,
                        phone: item.telephone || null,
                        email: item.email || null,
                        image: item.image?.url || item.image || null,
                        rating: item.aggregateRating?.ratingValue || null,
                        reviewCount: item.aggregateRating?.reviewCount || null,
                        address: item.address ? {
                            street: item.address.streetAddress || null,
                            city: item.address.addressLocality || null,
                            state: item.address.addressRegion || null,
                            zip: item.address.postalCode || null,
                        } : null,
                    };
                    break;
                }
            }
        } catch { /* ignore */ }
    });
    return data;
};

const extractFromHtml = ($) => ({
    name: cleanText($('h1').first().text()) || null,
    specialty: cleanText($('[class*="specialty"]').first().text()) || null,
    bio: cleanText($('[class*="bio"], [class*="about"]').first().text()) || null,
    phone: $('a[href^="tel:"]').first().text().trim() || null,
    email: $('a[href^="mailto:"]').attr('href')?.replace('mailto:', '').trim() || null,
    image: $('img[class*="photo"], img[class*="profile"]').attr('src') || null,
    education: cleanText($('[class*="education"]').text()) || null,
    insurance: cleanText($('[class*="insurance"]').text()) || null,
});

// ============================================================================
// MAIN ACTOR
// ============================================================================

await Actor.init();

try {
    const input = (await Actor.getInput()) || {};
    const {
        specialty = 'Cardiovascular Disease',
        location = 'New York, NY',
        collectDetails = true,
        results_wanted: resultsWantedRaw = 50,
        max_pages: maxPagesRaw = 5,
        maxConcurrency = 2,
        proxyConfiguration: proxyConfig,
    } = input;

    const resultsWanted = Number.isFinite(+resultsWantedRaw) ? Math.max(1, +resultsWantedRaw) : 50;
    const maxPages = Number.isFinite(+maxPagesRaw) ? Math.max(1, +maxPagesRaw) : 5;

    const proxyConfiguration = proxyConfig
        ? await Actor.createProxyConfiguration(proxyConfig)
        : await Actor.createProxyConfiguration({ useApifyProxy: true });

    const kvStore = await KeyValueStore.open();
    const startTime = Date.now();
    const MAX_RUNTIME_MS = 4.5 * 60 * 1000;

    // Stats
    const stats = { pagesProcessed: 0, doctorsFound: 0, detailsFetched: 0, errors: 0, httpSuccess: 0, playwrightFallback: 0 };
    const seenUrls = new Set();
    let savedCount = 0;

    // Session data for HTTP requests
    let sessionData = { cookies: [], userAgent: '' };

    log.info('='.repeat(65));
    log.info('🏥 VITALS.COM PHYSICIAN SCRAPER - HYBRID MODE');
    log.info('='.repeat(65));
    log.info(`🎯 Specialty: ${specialty} → ${getSpecialtySlug(specialty)}`);
    log.info(`📍 Location: ${location}`);
    log.info(`📊 Target: ${resultsWanted} physicians, max ${maxPages} pages`);
    log.info('='.repeat(65));

    // Build listing URLs
    const listingRequests = [];
    for (let page = 1; page <= maxPages; page++) {
        listingRequests.push({
            url: buildListingUrl({ specialty, location, page }),
            label: 'LISTING',
            userData: { page },
        });
    }

    // Detail URLs queue - populated from listings
    const detailRequests = [];

    log.info(`📄 Queued ${listingRequests.length} listing pages`);
    log.info(`   First URL: ${listingRequests[0]?.url}`);

    // ========================================================================
    // PLAYWRIGHT CRAWLER - LISTINGS + DETAILS
    // ========================================================================

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxConcurrency: Math.min(maxConcurrency, 2),
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 60,
        navigationTimeoutSecs: 45,

        launchContext: {
            launcher: firefox,
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-extensions',
                ],
            },
        },

        browserPoolOptions: {
            maxOpenPagesPerBrowser: 1,
            useFingerprints: true,
            fingerprintOptions: {
                fingerprintGeneratorOptions: {
                    browsers: ['firefox'],
                    operatingSystems: ['windows', 'macos'],
                    locales: ['en-US'],
                },
            },
        },

        preNavigationHooks: [
            async ({ page }) => {
                await page.setViewportSize({
                    width: 1920 + Math.floor(Math.random() * 80) - 40,
                    height: 1080 + Math.floor(Math.random() * 60) - 30,
                });

                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                });

                // Block heavy resources
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    const url = route.request().url();
                    if (['image', 'font', 'media'].includes(type)) return route.abort();
                    if (url.includes('google-analytics') || url.includes('googletagmanager') ||
                        url.includes('facebook') || url.includes('doubleclick')) return route.abort();
                    return route.continue();
                });
            },
        ],

        async requestHandler({ request, page, log: crawlerLog }) {
            const { url, label, userData } = request;

            // Timeout check
            if (Date.now() - startTime > MAX_RUNTIME_MS) {
                crawlerLog.info('⏱️ Timeout approaching, stopping...');
                return;
            }

            // Skip if we already have enough results
            if (savedCount >= resultsWanted) {
                crawlerLog.info(`✅ Target reached (${savedCount}/${resultsWanted}), skipping...`);
                return;
            }

            // Skip listing pages if we already have enough URLs queued
            if (label === 'LISTING' && detailRequests.length >= resultsWanted) {
                crawlerLog.info(`⏭️ Already have ${detailRequests.length} URLs queued, skipping listing page`);
                return;
            }

            try {
                await page.waitForLoadState('domcontentloaded', { timeout: 30000 });
                await page.waitForTimeout(1000 + Math.random() * 500);
            } catch (err) {
                crawlerLog.warning(`Load timeout: ${err.message}`);
            }

            const html = await page.content();
            const $ = cheerioLoad(html);

            if (label === 'LISTING') {
                crawlerLog.info(`🔄 Listing page ${userData.page}: ${url}`);
                stats.pagesProcessed++;

                // Scroll to load lazy content
                try {
                    await page.evaluate(async () => {
                        for (let i = 0; i < 3; i++) {
                            window.scrollTo(0, document.body.scrollHeight * (i + 1) / 3);
                            await new Promise(r => setTimeout(r, 300));
                        }
                    });
                    await page.waitForTimeout(500);
                } catch { /* ignore */ }

                // Re-get HTML after scroll
                const scrolledHtml = await page.content();
                const $scrolled = cheerioLoad(scrolledHtml);
                const doctors = extractDoctorsFromListing($scrolled);

                crawlerLog.info(`✅ Found ${doctors.length} doctors on page ${userData.page}`);
                stats.doctorsFound += doctors.length;

                // Save session cookies for HTTP layer
                if (sessionData.cookies.length === 0) {
                    try {
                        const context = page.context();
                        sessionData.cookies = await context.cookies();
                        sessionData.userAgent = await page.evaluate(() => navigator.userAgent);
                        await kvStore.setValue(SESSION_KEY, sessionData);
                        crawlerLog.info(`💾 Session saved: ${sessionData.cookies.length} cookies`);
                    } catch (err) {
                        crawlerLog.warning(`Session save failed: ${err.message}`);
                    }
                }

                // Queue detail pages (only up to resultsWanted)
                for (const doc of doctors) {
                    if (detailRequests.length >= resultsWanted) {
                        crawlerLog.info(`📊 Reached ${resultsWanted} URLs, stopping extraction`);
                        break;
                    }
                    if (seenUrls.has(doc.url)) continue;
                    seenUrls.add(doc.url);

                    if (collectDetails) {
                        detailRequests.push({
                            url: doc.url,
                            label: 'DETAIL',
                            userData: { doc },
                        });
                    } else {
                        // Save directly without details
                        await Dataset.pushData({
                            ...doc,
                            id: doc.url,
                            source: 'listing',
                            fetched_at: new Date().toISOString(),
                        });
                        savedCount++;
                        if (savedCount >= resultsWanted) break;
                    }
                }

                crawlerLog.info(`📊 Queued ${detailRequests.length}/${resultsWanted} details`);

            } else if (label === 'DETAIL') {
                stats.detailsFetched++;
                const doc = userData.doc;

                // Try JSON-LD first
                let details = extractJsonLd($);
                if (details) {
                    stats.httpSuccess++; // Count as structured data success
                } else {
                    details = extractFromHtml($);
                    stats.playwrightFallback++;
                }

                const record = {
                    id: doc.url,
                    name: details.name || doc.name,
                    specialty: details.specialty || doc.specialty || getSpecialtySlug(specialty),
                    location: details.address?.city
                        ? `${details.address.city}, ${details.address.state}`
                        : doc.location || location,
                    phone: details.phone || null,
                    email: details.email || null,
                    rating: details.rating || doc.rating || null,
                    reviewCount: details.reviewCount || null,
                    bio: details.bio || null,
                    education: details.education || null,
                    insurance: details.insurance || null,
                    image: details.image || null,
                    address: details.address || null,
                    url: doc.url,
                    source: details.name ? 'json-ld' : 'html',
                    fetched_at: new Date().toISOString(),
                };

                await Dataset.pushData(record);
                savedCount++;

                if (savedCount % 5 === 0 || savedCount === resultsWanted) {
                    crawlerLog.info(`📊 Progress: ${savedCount}/${resultsWanted} saved`);
                }
            }
        },

        async failedRequestHandler({ request, error, log: crawlerLog }) {
            stats.errors++;
            crawlerLog.error(`❌ Failed: ${request.url} - ${error.message}`);
        },
    });

    // Run on listing pages first
    log.info('🦊 Phase 1: Scraping listing pages...');
    await crawler.run(listingRequests);

    log.info(`🦊 Phase 1 Complete: Found ${detailRequests.length} doctor URLs`);

    // Run on detail pages if needed
    if (collectDetails && detailRequests.length > 0 && savedCount < resultsWanted) {
        const toProcess = detailRequests.slice(0, resultsWanted - savedCount);
        log.info(`⚡ Phase 2: Fetching ${toProcess.length} detail pages...`);
        await crawler.run(toProcess);
    }

    // ========================================================================
    // FINAL REPORT
    // ========================================================================

    const totalTime = (Date.now() - startTime) / 1000;
    const rate = savedCount > 0 ? (savedCount / totalTime).toFixed(2) : '0';

    log.info('='.repeat(65));
    log.info('📊 EXECUTION REPORT');
    log.info('='.repeat(65));
    log.info(`📄 Listing pages: ${stats.pagesProcessed}`);
    log.info(`👨‍⚕️ Doctors found: ${stats.doctorsFound}`);
    log.info(`🔍 Details fetched: ${stats.detailsFetched}`);
    log.info(`✅ JSON-LD extractions: ${stats.httpSuccess}`);
    log.info(`🔧 HTML fallbacks: ${stats.playwrightFallback}`);
    log.info(`⚠️ Errors: ${stats.errors}`);
    log.info('='.repeat(65));
    log.info(`✅ Total saved: ${savedCount}/${resultsWanted}`);
    log.info(`⏱️ Runtime: ${totalTime.toFixed(2)}s`);
    log.info(`⚡ Rate: ${rate} physicians/sec`);
    log.info('='.repeat(65));

    if (savedCount === 0) {
        await Actor.fail('No results scraped. Check selectors or anti-bot detection.');
    } else {
        await Actor.setValue('OUTPUT_SUMMARY', {
            pagesProcessed: stats.pagesProcessed,
            doctorsFound: stats.doctorsFound,
            detailsFetched: stats.detailsFetched,
            totalSaved: savedCount,
            errors: stats.errors,
            runtimeSeconds: totalTime,
            success: true,
        });
    }

} catch (error) {
    log.error(`❌ CRITICAL: ${error.message}`);
    log.exception(error, 'Actor failed');
    throw error;
} finally {
    await Actor.exit();
}
