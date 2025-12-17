// Vitals.com Physician Scraper - Production Ready, Fast & Stealthy
import { Actor, log } from 'apify';
import { Dataset, gotScraping } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

const BASE_URL = 'https://www.vitals.com';
const SEARCH_PAGE_BASE = 'https://www.vitals.com/doctors';

// Stealth User-Agents - rotated per request
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
];

const getRandomUserAgent = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

const getStealthHeaders = () => ({
    'User-Agent': getRandomUserAgent(),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/*;q=0.8,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
    'Referer': 'https://www.google.com/',
});

const cleanHtml = (html) => {
    if (!html) return null;
    const $ = cheerioLoad(html);
    $('script, style, noscript').remove();
    return $.root().text().replace(/\s+/g, ' ').trim();
};

const createLimiter = (maxConcurrency) => {
    let active = 0;
    const queue = [];
    const next = () => {
        if (active >= maxConcurrency || queue.length === 0) return;
        active += 1;
        const { task, resolve, reject } = queue.shift();
        task()
            .then((res) => {
                resolve(res);
            })
            .catch((err) => {
                reject(err);
            })
            .finally(() => {
                active -= 1;
                next();
            });
    };
    return (task) =>
        new Promise((resolve, reject) => {
            queue.push({ task, resolve, reject });
            next();
        });
};

const buildSearchUrl = ({ specialty, location, insurance, page = 1 }) => {
    const u = new URL(SEARCH_PAGE_BASE);
    if (specialty) u.searchParams.set('specialty', specialty);
    if (location) u.searchParams.set('location', location);
    if (insurance) u.searchParams.set('insurance', insurance);
    u.searchParams.set('page', String(page));
    return u.href;
};

const pickProxyUrl = async (proxyConfiguration) => (proxyConfiguration ? proxyConfiguration.newUrl() : undefined);

// Exponential backoff retry mechanism
const fetchWithRetry = async (options, maxRetries = 3) => {
    let lastError = null;
    for (let attempt = 0; attempt < maxRetries; attempt += 1) {
        try {
            const delay = Math.random() * (500 * Math.pow(2, attempt));
            if (attempt > 0) {
                await new Promise((resolve) => setTimeout(resolve, delay));
            }
            const res = await gotScraping(options);
            return res;
        } catch (err) {
            lastError = err;
            if (attempt < maxRetries - 1) {
                const delay = Math.random() * (1000 * Math.pow(2, attempt));
                log.warning(`Request failed (attempt ${attempt + 1}/${maxRetries}), retrying after ${delay.toFixed(0)}ms: ${err.message}`);
                await new Promise((resolve) => setTimeout(resolve, delay));
            }
        }
    }
    throw lastError;
};

const parsePhysiciansPage = (html, pageUrl) => {
    const $ = cheerioLoad(html);
    const physicians = [];

    // Primary selector: physician cards/listings
    const selectors = [
        '[data-testid*="doctor"]',
        '.doctor-card',
        '.provider-card',
        '.physician-listing',
        '[class*="DoctorCard"]',
        '[class*="PhysicianCard"]',
    ];

    let cardElements = [];
    for (const selector of selectors) {
        cardElements = $(selector).get();
        if (cardElements.length > 0) {
            log.debug(`Found ${cardElements.length} cards using selector: ${selector}`);
            break;
        }
    }

    if (cardElements.length === 0) {
        // Fallback: look for links to doctor profiles
        cardElements = $('a[href*="/doctors/"]').parent().get();
    }

    cardElements.slice(0, 50).forEach((el) => {
        try {
            const $card = $(el);
            const profileLink = $card.find('a[href*="/doctors/"]').attr('href') || 
                               $card.attr('href') || 
                               $card.find('a').first().attr('href');
            
            if (!profileLink || !profileLink.includes('/doctors/')) return;

            const fullUrl = new URL(profileLink, BASE_URL).href;
            const name = $card.find('[class*="name"], h2, h3, .title').first().text().trim() ||
                        $card.find('a[href*="/doctors/"]').first().text().trim();
            const specialty = $card.find('[class*="specialty"], [class*="specialization"]').first().text().trim();
            const location = $card.find('[class*="location"], [class*="city"]').first().text().trim();
            const rating = $card.find('[class*="rating"], [class*="score"]').first().text().match(/[\d.]+/)?.[0];
            const phone = $card.find('a[href^="tel:"]').attr('href')?.replace('tel:', '').trim();

            if (name) {
                physicians.push({
                    id: fullUrl,
                    name,
                    specialty: specialty || null,
                    location: location || null,
                    phone: phone || null,
                    rating: rating ? parseFloat(rating) : null,
                    url: fullUrl,
                    source: 'html-search',
                    fetched_at: new Date().toISOString(),
                });
            }
        } catch (err) {
            log.debug(`Error parsing card element: ${err.message}`);
        }
    });

    return physicians;
};

const fetchSearchPage = async (params, pageNumber, proxyConfiguration) => {
    try {
        const searchUrl = buildSearchUrl({ 
            specialty: params.specialty, 
            location: params.location, 
            insurance: params.insurance,
            page: pageNumber 
        });

        log.debug(`Fetching search page ${pageNumber}: ${searchUrl}`);

        const res = await fetchWithRetry({
            url: searchUrl,
            headers: getStealthHeaders(),
            responseType: 'text',
            proxyUrl: await pickProxyUrl(proxyConfiguration),
            timeout: { request: 30000 },
            throwHttpErrors: false,
        });

        if (res.statusCode >= 400) {
            log.warning(`Search page ${pageNumber} returned ${res.statusCode}`);
            return { physicians: [], hasMore: false };
        }

        const physicians = parsePhysiciansPage(res.body, searchUrl);
        const $ = cheerioLoad(res.body);
        const hasNextPage = $('a[rel="next"]').length > 0 || 
                           $('a:contains("Next")').length > 0 ||
                           physicians.length >= 20;

        return { physicians, hasMore: hasNextPage };
    } catch (err) {
        log.error(`Failed to fetch search page ${pageNumber}: ${err.message}`);
        return { physicians: [], hasMore: false };
    }
};

const fetchPhysicianDetail = async (physicianUrl, proxyConfiguration) => {
    try {
        const res = await fetchWithRetry({
            url: physicianUrl,
            headers: getStealthHeaders(),
            responseType: 'text',
            proxyUrl: await pickProxyUrl(proxyConfiguration),
            timeout: { request: 30000 },
            throwHttpErrors: false,
        }, 2);

        if (res.statusCode >= 400) {
            return null;
        }

        const $ = cheerioLoad(res.body);
        
        // Extract JSON-LD data if available
        let ldData = null;
        $('script[type="application/ld+json"]').each((_, el) => {
            try {
                const json = JSON.parse($(el).contents().text().trim());
                const person = Array.isArray(json) 
                    ? json.find((j) => j['@type'] === 'Person' || j['@type'] === 'MedicalBusiness')
                    : json;
                if (person && (person['@type'] === 'Person' || person['@type'] === 'MedicalBusiness')) {
                    ldData = person;
                }
            } catch {
                // ignore malformed JSON-LD
            }
        });

        const bio = ldData?.description || 
                   $('[class*="bio"]').first().text().trim() || 
                   $('[class*="about"]').first().text().trim() || null;
        
        const phone = ldData?.telephone || 
                     $('a[href^="tel:"]').first().text().trim() || 
                     null;
        
        const education = ldData?.educationalBackground || 
                         $('[class*="education"]').html() || 
                         null;
        
        const insurance = $('[class*="insurance"]').text().trim() || null;

        return {
            bio: bio ? cleanHtml(bio) : null,
            phone: phone || null,
            email: ldData?.email || null,
            education,
            insurance,
            _ldJson: ldData || null,
        };
    } catch (err) {
        log.debug(`Failed to fetch detail for ${physicianUrl}: ${err.message}`);
        return null;
    }
};

const buildPhysician = ({ listing, detail }) => ({
    id: listing.id,
    name: listing.name || null,
    specialty: listing.specialty || null,
    location: listing.location || null,
    phone: detail?.phone || listing.phone || null,
    email: detail?.email || null,
    rating: listing.rating || null,
    bio: detail?.bio || null,
    education: detail?.education || null,
    insurance: detail?.insurance || null,
    url: listing.url,
    source: listing.source || 'html',
    fetched_at: new Date().toISOString(),
});

// Initialize Actor properly for Apify platform
await Actor.init();

try {
    const input = (await Actor.getInput()) || {};
    const {
        startUrl,
        specialty = 'Cardiovascular Disease',
        location = 'United States',
        insurance = '',
        collectDetails = true,
        results_wanted: resultsWantedRaw = 50,
        max_pages: maxPagesRaw = 5,
        maxConcurrency = 3,
        proxyConfiguration,
    } = input;

    const resultsWanted = Number.isFinite(+resultsWantedRaw) ? Math.max(1, +resultsWantedRaw) : 1;
    const maxPages = Number.isFinite(+maxPagesRaw) ? Math.max(1, +maxPagesRaw) : 1;
    const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

    let specialtyValue = specialty;
    let locationValue = location;

    if (startUrl) {
        try {
            const u = new URL(startUrl);
            const fromSpecialty = u.searchParams.get('specialty');
            const fromLocation = u.searchParams.get('location');
            if (fromSpecialty) specialtyValue = fromSpecialty;
            if (fromLocation) locationValue = fromLocation;
        } catch {
            // ignore malformed startUrl
        }
    }

    const seenIds = new Set();
    const limiter = createLimiter(Math.max(1, Number(maxConcurrency) || 1));
    const startTime = Date.now();
    const MAX_RUNTIME_MS = 3.5 * 60 * 1000;
    
    let saved = 0;
    let pageNumber = 1;
    let stats = { pagesProcessed: 0, detailsFetched: 0, errors: 0, totalApiCalls: 0 };

    log.info(`🚀 Starting scrape: specialty="${specialtyValue}", location="${locationValue}", target=${resultsWanted}`);

    // Main pagination loop - FAST iteration
    while (pageNumber <= maxPages && saved < resultsWanted) {
        const elapsed = Date.now() - startTime;
        if (elapsed > MAX_RUNTIME_MS) {
            log.info(`⏱️  Timeout safety: Stopping at ${(elapsed / 1000).toFixed(0)}s. Saved ${saved} physicians.`);
            break;
        }

        try {
            log.info(`📄 Fetching page ${pageNumber}...`);
            const { physicians, hasMore } = await fetchSearchPage(
                { specialty: specialtyValue, location: locationValue, insurance },
                pageNumber,
                proxyConf
            );
            stats.totalApiCalls += 1;
            stats.pagesProcessed = pageNumber;

            if (!physicians || physicians.length === 0) {
                log.info(`✅ No more results on page ${pageNumber}. Stopping pagination.`);
                break;
            }

            log.info(`✅ Page ${pageNumber}: Found ${physicians.length} physicians (total: ${saved}/${resultsWanted})`);

            // Parallel detail fetching with concurrency limiter
            const remaining = resultsWanted - saved;
            const detailPromises = physicians.slice(0, remaining).map((listing) =>
                limiter(async () => {
                    if (saved >= resultsWanted) return;
                    if (seenIds.has(listing.id)) return;
                    seenIds.add(listing.id);

                    try {
                        let detail = null;
                        if (collectDetails && listing.url) {
                            detail = await fetchPhysicianDetail(listing.url, proxyConf);
                            stats.detailsFetched += 1;
                            stats.totalApiCalls += 1;
                            
                            // Random delay between requests for stealth
                            await new Promise((resolve) => 
                                setTimeout(resolve, 200 + Math.random() * 300)
                            );
                        }

                        const physician = buildPhysician({ listing, detail });
                        await Dataset.pushData(physician);
                        saved += 1;
                    } catch (err) {
                        stats.errors += 1;
                        log.warning(`Failed to process ${listing.name}: ${err.message}`);
                    }
                }),
            );

            await Promise.all(detailPromises);

            if (!hasMore || saved >= resultsWanted) {
                log.info(`✅ Pagination complete. Total physicians saved: ${saved}/${resultsWanted}`);
                break;
            }

            pageNumber += 1;

            // Dynamic delay between page requests (anti-detection)
            const pageDelay = 500 + Math.random() * 1500;
            await new Promise((resolve) => setTimeout(resolve, pageDelay));
        } catch (err) {
            stats.errors += 1;
            log.error(`Error on page ${pageNumber}: ${err.message}`);
            
            // Retry logic: skip failed page and continue
            if (pageNumber < maxPages) {
                log.info(`Retrying page ${pageNumber + 1}...`);
                pageNumber += 1;
            } else {
                break;
            }
        }
    }

    const totalTime = (Date.now() - startTime) / 1000;
    const performanceRate = saved > 0 ? (saved / totalTime).toFixed(2) : '0';

    // Final statistics report
    log.info('='.repeat(65));
    log.info('📊 ACTOR EXECUTION REPORT');
    log.info('='.repeat(65));
    log.info(`✅ Physicians scraped: ${saved}/${resultsWanted}`);
    log.info(`📄 Pages processed: ${stats.pagesProcessed}/${maxPages}`);
    log.info(`🔍 Detail pages fetched: ${stats.detailsFetched}`);
    log.info(`📡 Total API requests: ${stats.totalApiCalls}`);
    log.info(`⚠️  Errors: ${stats.errors}`);
    log.info(`⏱️  Total runtime: ${totalTime.toFixed(2)}s`);
    log.info(`⚡ Performance: ${performanceRate} physicians/second`);
    log.info(`🎯 Success rate: ${((saved / resultsWanted) * 100).toFixed(1)}%`);
    log.info('='.repeat(65));

    // Final validation
    if (saved === 0) {
        const errorMsg = 'CRITICAL: No results scraped. Verify search parameters and network connectivity.';
        log.error(`❌ ${errorMsg}`);
        await Actor.fail(errorMsg);
    } else {
        log.info(`✅ SUCCESS: Scraped ${saved} physicians in ${totalTime.toFixed(2)}s`);
        await Actor.setValue('OUTPUT_SUMMARY', {
            physiciansScrape: saved,
            pagesProcessed: stats.pagesProcessed,
            detailsFetched: stats.detailsFetched,
            totalApiCalls: stats.totalApiCalls,
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
