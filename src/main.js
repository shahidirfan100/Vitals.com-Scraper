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
    // Try multiple URL formats for better compatibility
    let searchUrl;
    
    // Format 1: Modern search endpoint
    const u = new URL('https://www.vitals.com/doctors');
    if (specialty) u.searchParams.set('specialty', specialty);
    if (location) u.searchParams.set('location', location);
    if (insurance) u.searchParams.set('insurance', insurance);
    u.searchParams.set('page', String(page));
    
    searchUrl = u.href;
    log.debug(`Built search URL: ${searchUrl}`);
    return searchUrl;
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

    // Method 1: Try common card selectors
    const selectors = [
        '[data-testid*="doctor"]',
        '[data-testid*="provider"]',
        '.doctor-card',
        '.provider-card',
        '.physician-listing',
        '[class*="DoctorCard"]',
        '[class*="PhysicianCard"]',
        '[class*="ProviderCard"]',
        'article',
        'div[role="listitem"]',
    ];

    let cardElements = [];
    let foundSelector = null;
    for (const selector of selectors) {
        const elements = $(selector).get();
        if (elements.length > 0) {
            cardElements = elements;
            foundSelector = selector;
            log.debug(`Found ${elements.length} cards using selector: ${selector}`);
            break;
        }
    }

    // Method 2: If no cards found, extract from all links to doctor profiles
    if (cardElements.length === 0) {
        log.debug(`No card elements found, extracting from doctor profile links...`);
        const doctorLinks = $('a[href*="/doctors/"]').get();
        log.debug(`Found ${doctorLinks.length} doctor profile links`);
        
        if (doctorLinks.length > 0) {
            // Group links by parent container for better data extraction
            const seenUrls = new Set();
            doctorLinks.forEach((linkEl) => {
                try {
                    const href = $(linkEl).attr('href');
                    if (!href || seenUrls.has(href)) return;
                    seenUrls.add(href);

                    const fullUrl = new URL(href, BASE_URL).href;
                    const name = $(linkEl).text().trim();
                    
                    if (name && name.length > 2) {
                        // Look for parent container with more info
                        const $parent = $(linkEl).closest('div, section, article, li');
                        const specialty = $parent.find('[class*="specialty"], [class*="specialization"]').text().trim();
                        const location = $parent.find('[class*="location"], [class*="city"], [class*="address"]').text().trim();
                        const ratingText = $parent.find('[class*="rating"], [class*="score"], [class*="stars"]').text();
                        const rating = ratingText.match(/[\d.]+/)?.[0];

                        physicians.push({
                            id: fullUrl,
                            name,
                            specialty: specialty || null,
                            location: location || null,
                            phone: null,
                            rating: rating ? parseFloat(rating) : null,
                            url: fullUrl,
                            source: 'html-search',
                            fetched_at: new Date().toISOString(),
                        });
                    }
                } catch (err) {
                    log.debug(`Error processing doctor link: ${err.message}`);
                }
            });
            return physicians;
        }
    }

    // Method 3: Process card elements
    cardElements.slice(0, 50).forEach((el) => {
        try {
            const $card = $(el);
            
            // Try to find doctor profile link within card
            const $link = $card.find('a[href*="/doctors/"]').first();
            let profileLink = $link.attr('href');
            let name = $link.text().trim();

            // If no link found, try broader approach
            if (!profileLink) {
                profileLink = $card.find('a').first().attr('href');
                if (!profileLink || !profileLink.includes('/doctors/')) {
                    // Last resort: extract from any a tag
                    const allLinks = $card.find('a').get();
                    for (const link of allLinks) {
                        const href = $(link).attr('href');
                        if (href && href.includes('/doctors/')) {
                            profileLink = href;
                            name = $(link).text().trim();
                            break;
                        }
                    }
                }
            }
            
            if (!profileLink || !profileLink.includes('/doctors/')) return;

            const fullUrl = new URL(profileLink, BASE_URL).href;
            
            // Extract name if not already found
            if (!name) {
                name = $card.find('h1, h2, h3, h4, [class*="name"], [class*="title"]').first().text().trim();
            }

            const specialty = $card.find('[class*="specialty"], [class*="specialization"], span').first().text().trim();
            const location = $card.find('[class*="location"], [class*="city"], [class*="address"]').text().trim();
            const ratingText = $card.find('[class*="rating"], [class*="score"], [class*="stars"]').text();
            const rating = ratingText.match(/[\d.]+/)?.[0];
            const phone = $card.find('a[href^="tel:"]').attr('href')?.replace('tel:', '').trim();

            if (name && name.length > 2) {
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

    log.info(`parsePhysiciansPage: Extracted ${physicians.length} physicians (selector: ${foundSelector || 'links'})`);
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

        log.info(`Fetching: ${searchUrl}`);

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

        // Check if HTML content is valid and contains doctor links
        if (!res.body || res.body.length < 1000) {
            log.warning(`Page ${pageNumber} returned minimal content (${res.body?.length || 0} bytes)`);
            return { physicians: [], hasMore: false };
        }

        // Log first 500 chars for debugging
        log.debug(`Response preview: ${res.body.substring(0, 500)}`);

        const physicians = parsePhysiciansPage(res.body, searchUrl);
        const $ = cheerioLoad(res.body);
        
        // Check for pagination indicators
        const hasNextPage = $('a[rel="next"]').length > 0 || 
                           $('a:contains("Next")').length > 0 ||
                           $('a[href*="page="]').length > 0 ||
                           physicians.length >= 15; // If we got significant results, assume more pages

        log.info(`Page ${pageNumber}: Got ${physicians.length} physicians, hasMore: ${hasNextPage}`);
        
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
            log.info(`📄 Fetching page ${pageNumber}/${maxPages}...`);
            const { physicians, hasMore } = await fetchSearchPage(
                { specialty: specialtyValue, location: locationValue, insurance },
                pageNumber,
                proxyConf
            );
            stats.totalApiCalls += 1;
            stats.pagesProcessed = pageNumber;

            log.info(`Received ${physicians.length} physicians from page ${pageNumber}`);

            if (!physicians || physicians.length === 0) {
                if (pageNumber === 1) {
                    log.warning(`⚠️  First page returned 0 results. Retrying with different approach...`);
                    // Try one more time on first page
                    const retry = await fetchSearchPage(
                        { specialty: specialtyValue, location: locationValue, insurance },
                        pageNumber,
                        proxyConf
                    );
                    if (retry.physicians.length === 0) {
                        log.error(`❌ Search returned no results after retry. This suggests the search URL or selectors need updating.`);
                        break;
                    } else {
                        physicians.push(...retry.physicians);
                    }
                } else {
                    log.info(`✅ No more results on page ${pageNumber}. Stopping pagination.`);
                    break;
                }
            }

            if (physicians.length === 0) break;

            log.info(`✅ Page ${pageNumber}: Found ${physicians.length} physicians (total saved: ${saved}/${resultsWanted})`);

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
