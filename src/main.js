// Vitals.com Physician Scraper - Production Ready, Fast & Stealthy
// Hybrid approach: PlaywrightCrawler for Cloudflare bypass + Multi-tier extraction
import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

const BASE_URL = 'https://www.vitals.com';

// Specialty slug mappings for common specialties
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
    'pain management': 'pain-management-specialists',
    'physical therapy': 'physical-therapists',
    'chiropractor': 'chiropractors',
    'podiatrist': 'podiatrists',
    'optometrist': 'optometrists',
};

// State abbreviation mappings
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

/**
 * Convert specialty input to URL slug
 */
const getSpecialtySlug = (specialty) => {
    if (!specialty) return 'doctors';
    const normalized = specialty.toLowerCase().trim();
    return SPECIALTY_SLUGS[normalized] || normalized.replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
};

/**
 * Parse location into state abbreviation and city slug
 */
const parseLocation = (location) => {
    if (!location) return null;
    const normalized = location.toLowerCase().trim();

    // Priority 1: Try to parse "City, State" or "City, ST" format
    if (normalized.includes(',')) {
        const [cityPart, statePart] = normalized.split(',').map(s => s.trim());
        if (cityPart && statePart) {
            // Check if statePart is a state abbreviation or full name
            const stateAbbrev = statePart.length === 2
                ? statePart
                : STATE_ABBREVIATIONS[statePart];
            if (stateAbbrev) {
                const city = cityPart.replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
                return { state: stateAbbrev, city: city || null };
            }
        }
    }

    // Priority 2: Try "City State" or "City ST" (space-separated)
    const parts = normalized.split(/\s+/);
    if (parts.length >= 2) {
        const lastPart = parts[parts.length - 1];
        const stateAbbrev = lastPart.length === 2
            ? lastPart
            : STATE_ABBREVIATIONS[lastPart];
        if (stateAbbrev) {
            const city = parts.slice(0, -1).join('-').replace(/[^a-z0-9-]/g, '');
            return { state: stateAbbrev, city: city || null };
        }
    }

    // Priority 3: Check if entire string is a state name
    if (STATE_ABBREVIATIONS[normalized]) {
        return { state: STATE_ABBREVIATIONS[normalized], city: null };
    }

    // Priority 4: Check if it's just a state abbreviation
    if (normalized.length === 2 && Object.values(STATE_ABBREVIATIONS).includes(normalized)) {
        return { state: normalized, city: null };
    }

    return null;
};

/**
 * Build listing URL for specialty/location
 */
const buildListingUrl = ({ specialty, location, page = 1 }) => {
    const specialtySlug = getSpecialtySlug(specialty);
    const locationInfo = parseLocation(location);

    let url;
    // Validate city is not empty or just hyphens/dashes
    const validCity = locationInfo?.city && locationInfo.city.replace(/-/g, '').length > 0;

    if (locationInfo?.state && validCity) {
        url = `${BASE_URL}/${specialtySlug}/${locationInfo.state}/${locationInfo.city}`;
    } else if (locationInfo?.state) {
        url = `${BASE_URL}/${specialtySlug}/${locationInfo.state}`;
    } else {
        url = `${BASE_URL}/${specialtySlug}`;
    }

    if (page > 1) {
        url += `?page=${page}`;
    }

    return url;
};

/**
 * Extract JSON-LD data from page (Priority 1)
 */
const extractJsonLd = (page, $) => {
    const results = [];

    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const jsonText = $(el).html();
            if (!jsonText) return;

            const data = JSON.parse(jsonText);
            const items = Array.isArray(data) ? data : [data];

            for (const item of items) {
                // Handle MedicalBusiness, Physician, Person types
                if (['MedicalBusiness', 'Physician', 'Person', 'LocalBusiness'].includes(item['@type'])) {
                    results.push({
                        name: item.name || null,
                        specialty: item.medicalSpecialty?.name || item.specialty || null,
                        description: item.description || null,
                        phone: item.telephone || null,
                        email: item.email || null,
                        url: item.url || null,
                        image: item.image?.url || item.image || null,
                        address: item.address ? {
                            street: item.address.streetAddress || null,
                            city: item.address.addressLocality || null,
                            state: item.address.addressRegion || null,
                            zip: item.address.postalCode || null,
                        } : null,
                        rating: item.aggregateRating?.ratingValue || null,
                        reviewCount: item.aggregateRating?.reviewCount || null,
                        _source: 'json-ld',
                        _rawLd: item,
                    });
                }

                // Handle ItemList (search results)
                if (item['@type'] === 'ItemList' && item.itemListElement) {
                    for (const listItem of item.itemListElement) {
                        const entity = listItem.item || listItem;
                        if (entity.name) {
                            results.push({
                                name: entity.name || null,
                                specialty: entity.medicalSpecialty?.name || null,
                                url: entity.url || null,
                                image: entity.image?.url || entity.image || null,
                                _source: 'json-ld-list',
                            });
                        }
                    }
                }
            }
        } catch (err) {
            log.debug(`JSON-LD parse error: ${err.message}`);
        }
    });

    return results;
};

/**
 * Extract physician data from HTML (Priority 2 - Fallback)
 */
const extractFromHtml = ($, pageUrl) => {
    const physicians = [];
    const seenUrls = new Set();

    // Common card/listing selectors for Vitals.com
    const cardSelectors = [
        '[data-testid*="provider"]',
        '[data-testid*="doctor"]',
        '[class*="ProviderCard"]',
        '[class*="DoctorCard"]',
        '[class*="provider-card"]',
        '[class*="doctor-card"]',
        'article[class*="provider"]',
        'article[class*="doctor"]',
        '.search-results-list > div',
        '.provider-listing',
        'a[href*="/doctors/Dr-"]',
        'a[href*="/doctors/dr-"]',
    ];

    // Try each selector to find doctor cards
    for (const selector of cardSelectors) {
        const elements = $(selector);
        if (elements.length === 0) continue;

        log.debug(`Found ${elements.length} elements with selector: ${selector}`);

        elements.each((_, el) => {
            try {
                const $el = $(el);

                // Find doctor profile link
                let profileUrl = null;
                let name = null;

                // If element is itself a link
                if ($el.is('a') && $el.attr('href')?.includes('/doctors/')) {
                    profileUrl = $el.attr('href');
                    name = $el.text().trim();
                } else {
                    // Find link within element
                    const $link = $el.find('a[href*="/doctors/"]').first();
                    if ($link.length) {
                        profileUrl = $link.attr('href');
                        name = $link.text().trim() || $el.find('h2, h3, h4, [class*="name"]').first().text().trim();
                    }
                }

                if (!profileUrl || seenUrls.has(profileUrl)) return;
                seenUrls.add(profileUrl);

                const fullUrl = profileUrl.startsWith('http') ? profileUrl : `${BASE_URL}${profileUrl}`;

                // Extract additional info from card
                const specialty = $el.find('[class*="specialty"], [class*="specialization"]').text().trim() ||
                    $el.find('span').filter((_, s) => $(s).text().match(/MD|DO|MBBS|Specialist/i)).text().trim();

                const location = $el.find('[class*="location"], [class*="address"], [class*="city"]').text().trim();

                const ratingText = $el.find('[class*="rating"], [class*="score"], [class*="stars"]').text();
                const ratingMatch = ratingText.match(/(\d+\.?\d*)/);
                const rating = ratingMatch ? parseFloat(ratingMatch[1]) : null;

                const reviewText = $el.find('[class*="review"]').text();
                const reviewMatch = reviewText.match(/(\d+)/);
                const reviewCount = reviewMatch ? parseInt(reviewMatch[1], 10) : null;

                const phone = $el.find('a[href^="tel:"]').attr('href')?.replace('tel:', '') || null;

                const image = $el.find('img').attr('src') || null;

                if (name && name.length > 2) {
                    physicians.push({
                        name,
                        specialty: specialty || null,
                        location: location || null,
                        phone,
                        rating,
                        reviewCount,
                        url: fullUrl,
                        image,
                        _source: 'html',
                    });
                }
            } catch (err) {
                log.debug(`Error parsing card: ${err.message}`);
            }
        });

        if (physicians.length > 0) break; // Found results, stop trying other selectors
    }

    // Fallback: Extract all doctor profile links if no cards found
    if (physicians.length === 0) {
        log.debug('No cards found, extracting all doctor links...');
        $('a[href*="/doctors/"]').each((_, el) => {
            try {
                const $link = $(el);
                const href = $link.attr('href');

                if (!href || seenUrls.has(href)) return;
                if (!href.match(/\/doctors\/[Dd]r-/)) return; // Must be doctor profile

                seenUrls.add(href);
                const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
                const name = $link.text().trim();

                if (name && name.length > 2 && !name.includes('View') && !name.includes('More')) {
                    physicians.push({
                        name,
                        url: fullUrl,
                        _source: 'html-links',
                    });
                }
            } catch (err) {
                log.debug(`Error extracting link: ${err.message}`);
            }
        });
    }

    return physicians;
};

/**
 * Extract detailed info from physician profile page
 */
const extractPhysicianDetail = ($, url) => {
    // Try JSON-LD first
    const jsonLdData = extractJsonLd(null, $);
    if (jsonLdData.length > 0) {
        const ld = jsonLdData[0];
        return {
            bio: ld.description || $('[class*="bio"], [class*="about"]').first().text().trim() || null,
            phone: ld.phone || $('a[href^="tel:"]').first().text().trim() || null,
            email: ld.email || null,
            address: ld.address || null,
            rating: ld.rating,
            reviewCount: ld.reviewCount,
            image: ld.image,
            education: $('[class*="education"]').text().trim() || null,
            insurance: $('[class*="insurance"]').text().trim() || null,
            _ldJson: ld._rawLd,
        };
    }

    // Fallback to HTML extraction
    return {
        bio: $('[class*="bio"], [class*="about"], [class*="description"]').first().text().trim() || null,
        phone: $('a[href^="tel:"]').first().text().trim() || null,
        email: $('a[href^="mailto:"]').first().text().replace('mailto:', '').trim() || null,
        address: $('[class*="address"]').text().trim() || null,
        rating: null,
        reviewCount: null,
        image: $('img[class*="photo"], img[class*="profile"]').attr('src') || null,
        education: $('[class*="education"]').text().trim() || null,
        insurance: $('[class*="insurance"]').text().trim() || null,
    };
};

/**
 * Check for pagination (next page)
 */
const hasNextPage = ($, currentPage) => {
    // Check various pagination indicators
    const hasNextLink = $('a[rel="next"]').length > 0 ||
        $('a:contains("Next")').length > 0 ||
        $(`a[href*="page=${currentPage + 1}"]`).length > 0 ||
        $('[class*="pagination"] a').filter((_, el) => $(el).text().includes('Next')).length > 0;

    return hasNextLink;
};

/**
 * Clean and sanitize text
 */
const cleanText = (text) => {
    if (!text) return null;
    return text.replace(/\s+/g, ' ').trim() || null;
};

// ============================================================================
// MAIN ACTOR
// ============================================================================

await Actor.init();

try {
    const input = (await Actor.getInput()) || {};
    const {
        startUrl,
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

    // Setup proxy
    const proxyConfiguration = proxyConfig
        ? await Actor.createProxyConfiguration(proxyConfig)
        : await Actor.createProxyConfiguration({ useApifyProxy: true });

    // Parse startUrl if provided
    let specialtyValue = specialty;
    let locationValue = location;

    if (startUrl) {
        try {
            const urlObj = new URL(startUrl);
            // Extract specialty from path like /dermatologists or /cardiologists
            const pathParts = urlObj.pathname.split('/').filter(Boolean);
            if (pathParts.length > 0) {
                specialtyValue = pathParts[0];
            }
            if (pathParts.length > 1) {
                locationValue = pathParts.slice(1).join(', ');
            }
        } catch {
            log.warning(`Could not parse startUrl: ${startUrl}`);
        }
    }

    // Tracking
    const seenUrls = new Set();
    let savedCount = 0;
    let currentPage = 1;
    const stats = { pagesProcessed: 0, detailsFetched: 0, errors: 0, jsonLdHits: 0, htmlFallbacks: 0 };
    const startTime = Date.now();
    const MAX_RUNTIME_MS = 4 * 60 * 1000; // 4 minute safety timeout

    // Detail page queue
    const detailQueue = [];

    log.info('='.repeat(65));
    log.info('🏥 VITALS.COM PHYSICIAN SCRAPER');
    log.info('='.repeat(65));
    log.info(`🎯 Specialty: ${specialtyValue}`);
    log.info(`📍 Location: ${locationValue}`);
    log.info(`📊 Target: ${resultsWanted} physicians, max ${maxPages} pages`);
    log.info('='.repeat(65));

    // Build initial URLs
    const startUrls = [];
    for (let page = 1; page <= maxPages && savedCount < resultsWanted; page++) {
        startUrls.push({
            url: buildListingUrl({ specialty: specialtyValue, location: locationValue, page }),
            label: 'LISTING',
            userData: { page },
        });
    }

    log.info(`📄 Starting with ${startUrls.length} listing page(s)`);
    log.info(`   First URL: ${startUrls[0]?.url}`);

    // Create Playwright crawler with stealth settings
    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxConcurrency: Math.max(1, Math.min(maxConcurrency, 3)), // Cap at 3 for stealth
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 60,
        navigationTimeoutSecs: 45,

        // Use Firefox for better Cloudflare bypass
        headless: true,
        launchContext: {
            launchOptions: {
                firefoxUserPrefs: {
                    'dom.webdriver.enabled': false,
                    'useAutomationExtension': false,
                },
            },
        },

        // Browser pool settings for Firefox
        browserPoolOptions: {
            useFingerprints: true,
            fingerprintOptions: {
                fingerprintGeneratorOptions: {
                    browsers: ['firefox'],
                    operatingSystems: ['windows', 'macos'],
                    locales: ['en-US'],
                },
            },
        },

        // Pre-navigation hook for stealth
        preNavigationHooks: [
            async ({ page }) => {
                // Set realistic viewport
                await page.setViewportSize({
                    width: 1366 + Math.floor(Math.random() * 200),
                    height: 768 + Math.floor(Math.random() * 200),
                });

                // Block unnecessary resources for speed
                await page.route('**/*', (route) => {
                    const resourceType = route.request().resourceType();
                    if (['image', 'font', 'media'].includes(resourceType)) {
                        route.abort();
                    } else {
                        route.continue();
                    }
                });
            },
        ],

        // Main request handler
        async requestHandler({ request, page, parseWithCheerio, log: crawlerLog }) {
            const { url, label, userData } = request;

            // Check timeout
            if (Date.now() - startTime > MAX_RUNTIME_MS) {
                crawlerLog.info(`⏱️ Timeout reached, stopping...`);
                return;
            }

            // Check results limit
            if (savedCount >= resultsWanted) {
                crawlerLog.info(`✅ Target reached (${savedCount}/${resultsWanted}), stopping...`);
                return;
            }

            crawlerLog.info(`🔄 Processing: ${url} [${label}]`);

            // Wait for page to load
            try {
                await page.waitForLoadState('domcontentloaded', { timeout: 30000 });
                await page.waitForTimeout(1000 + Math.random() * 1000); // Random delay
            } catch (err) {
                crawlerLog.warning(`Page load timeout: ${err.message}`);
            }

            const $ = await parseWithCheerio();

            if (label === 'LISTING') {
                stats.pagesProcessed++;

                // Try JSON-LD first (Priority 1)
                let physicians = extractJsonLd(page, $);
                if (physicians.length > 0) {
                    stats.jsonLdHits++;
                    crawlerLog.info(`✅ JSON-LD: Found ${physicians.length} physicians`);
                }

                // Fallback to HTML (Priority 2)
                if (physicians.length === 0) {
                    physicians = extractFromHtml($, url);
                    if (physicians.length > 0) {
                        stats.htmlFallbacks++;
                        crawlerLog.info(`✅ HTML: Found ${physicians.length} physicians`);
                    }
                }

                if (physicians.length === 0) {
                    crawlerLog.warning(`⚠️ No physicians found on page ${userData.page}`);

                    // Debug: Log page content snippet
                    const bodyText = $('body').text().slice(0, 500);
                    crawlerLog.debug(`Page content preview: ${bodyText}`);
                    return;
                }

                // Process found physicians
                for (const physician of physicians) {
                    if (savedCount >= resultsWanted) break;
                    if (!physician.url || seenUrls.has(physician.url)) continue;

                    seenUrls.add(physician.url);

                    if (collectDetails && physician.url) {
                        // Queue detail page for fetching
                        detailQueue.push(physician);
                    } else {
                        // Save directly without details
                        await Dataset.pushData({
                            ...physician,
                            specialty: cleanText(physician.specialty) || specialtyValue,
                            location: cleanText(physician.location) || locationValue,
                            fetched_at: new Date().toISOString(),
                        });
                        savedCount++;
                    }
                }

                crawlerLog.info(`📊 Progress: ${savedCount}/${resultsWanted} saved, ${detailQueue.length} pending details`);

            } else if (label === 'DETAIL') {
                stats.detailsFetched++;

                const detail = extractPhysicianDetail($, url);
                const listing = userData.listing || {};

                await Dataset.pushData({
                    id: url,
                    name: listing.name || cleanText($('h1').first().text()),
                    specialty: listing.specialty || cleanText($('[class*="specialty"]').first().text()) || specialtyValue,
                    location: detail.address?.city
                        ? `${detail.address.city}, ${detail.address.state}`
                        : listing.location || locationValue,
                    phone: detail.phone || listing.phone || null,
                    email: detail.email || null,
                    rating: detail.rating || listing.rating || null,
                    reviewCount: detail.reviewCount || listing.reviewCount || null,
                    bio: cleanText(detail.bio),
                    education: cleanText(detail.education),
                    insurance: cleanText(detail.insurance),
                    image: detail.image || listing.image || null,
                    address: detail.address || null,
                    url,
                    source: detail._ldJson ? 'json-ld' : 'html',
                    fetched_at: new Date().toISOString(),
                });
                savedCount++;
            }
        },

        // Error handler
        async failedRequestHandler({ request, error, log: crawlerLog }) {
            stats.errors++;
            crawlerLog.error(`❌ Failed: ${request.url} - ${error.message}`);
        },
    });

    // Run the crawler on listing pages first
    await crawler.run(startUrls);

    // Process detail pages if needed
    if (collectDetails && detailQueue.length > 0 && savedCount < resultsWanted) {
        log.info(`🔍 Fetching details for ${Math.min(detailQueue.length, resultsWanted - savedCount)} physicians...`);

        const detailRequests = detailQueue
            .slice(0, resultsWanted - savedCount)
            .map((listing) => ({
                url: listing.url,
                label: 'DETAIL',
                userData: { listing },
            }));

        await crawler.run(detailRequests);
    }

    // Final statistics
    const totalTime = (Date.now() - startTime) / 1000;
    const performanceRate = savedCount > 0 ? (savedCount / totalTime).toFixed(2) : '0';

    log.info('='.repeat(65));
    log.info('📊 ACTOR EXECUTION REPORT');
    log.info('='.repeat(65));
    log.info(`✅ Physicians scraped: ${savedCount}/${resultsWanted}`);
    log.info(`📄 Listing pages processed: ${stats.pagesProcessed}`);
    log.info(`🔍 Detail pages fetched: ${stats.detailsFetched}`);
    log.info(`📦 JSON-LD extractions: ${stats.jsonLdHits}`);
    log.info(`🔧 HTML fallbacks: ${stats.htmlFallbacks}`);
    log.info(`⚠️ Errors: ${stats.errors}`);
    log.info(`⏱️ Total runtime: ${totalTime.toFixed(2)}s`);
    log.info(`⚡ Performance: ${performanceRate} physicians/second`);
    log.info(`🎯 Success rate: ${((savedCount / resultsWanted) * 100).toFixed(1)}%`);
    log.info('='.repeat(65));

    // Final validation
    if (savedCount === 0) {
        const errorMsg = 'No results scraped. The website may be blocking requests or selectors need updating.';
        log.error(`❌ ${errorMsg}`);
        await Actor.fail(errorMsg);
    } else {
        log.info(`✅ SUCCESS: Scraped ${savedCount} physicians in ${totalTime.toFixed(2)}s`);
        await Actor.setValue('OUTPUT_SUMMARY', {
            physiciansScrape: savedCount,
            pagesProcessed: stats.pagesProcessed,
            detailsFetched: stats.detailsFetched,
            jsonLdHits: stats.jsonLdHits,
            htmlFallbacks: stats.htmlFallbacks,
            errors: stats.errors,
            runtimeSeconds: totalTime,
            performancePhysiciansPerSecond: parseFloat(performanceRate),
            success: true,
        });
    }
} catch (error) {
    log.error(`❌ CRITICAL ERROR: ${error.message}`);
    log.exception(error, 'Actor execution failed');
    throw error;
} finally {
    await Actor.exit();
}
