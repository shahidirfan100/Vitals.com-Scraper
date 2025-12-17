// Vitals.com Physician Scraper - JSON API first, HTML fallback, stealthy + fast.
// Priority order:
// 1) JSON endpoint (Next.js `/_next/data/...` style) -> parse JSON
// 2) Pure HTML (HTTP + JSON-LD/HTML parse)
// 3) Browser bootstrap (Playwright) only when Cloudflare blocks HTTP (to refresh cookies/buildId)
import { Actor, log } from 'apify';
import { Dataset, KeyValueStore } from 'crawlee';
import { gotScraping } from 'got-scraping';
import { load as cheerioLoad } from 'cheerio';
import { firefox } from 'playwright';

const BASE_URL = 'https://www.vitals.com';
const KV_KEY = 'VITALS_STATE_V1';

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:127.0) Gecko/20100101 Firefox/127.0',
];

const SPECIALTY_SLUGS = {
    'cardiovascular disease': 'cardiologists',
    cardiology: 'cardiologists',
    cardiologist: 'cardiologists',
    dermatology: 'dermatologists',
    dermatologist: 'dermatologists',
    'family medicine': 'family-medicine-doctors',
    'family practice': 'family-medicine-doctors',
    'internal medicine': 'internists',
    internist: 'internists',
    'orthopedic surgery': 'orthopedic-surgeons',
    orthopedics: 'orthopedic-surgeons',
    pediatrics: 'pediatricians',
    pediatrician: 'pediatricians',
    psychiatry: 'psychiatrists',
    psychiatrist: 'psychiatrists',
    neurology: 'neurologists',
    neurologist: 'neurologists',
    'obstetrics gynecology': 'obstetricians-gynecologists',
    'ob-gyn': 'obstetricians-gynecologists',
    ophthalmology: 'ophthalmologists',
    ophthalmologist: 'ophthalmologists',
    dentist: 'dentists',
    dentistry: 'dentists',
    gastroenterology: 'gastroenterologists',
    gastroenterologist: 'gastroenterologists',
    urology: 'urologists',
    urologist: 'urologists',
    pulmonology: 'pulmonologists',
    pulmonologist: 'pulmonologists',
    endocrinology: 'endocrinologists',
    endocrinologist: 'endocrinologists',
    rheumatology: 'rheumatologists',
    rheumatologist: 'rheumatologists',
    oncology: 'oncologists',
    oncologist: 'oncologists',
    'allergy immunology': 'allergists-immunologists',
    allergist: 'allergists-immunologists',
    'allergy-immunology': 'allergists-immunologists',
    'pain management': 'pain-management-specialists',
    'physical therapy': 'physical-therapists',
    chiropractor: 'chiropractors',
    podiatrist: 'podiatrists',
    optometrist: 'optometrists',
};

const STATE_ABBREVIATIONS = {
    alabama: 'al',
    alaska: 'ak',
    arizona: 'az',
    arkansas: 'ar',
    california: 'ca',
    colorado: 'co',
    connecticut: 'ct',
    delaware: 'de',
    florida: 'fl',
    georgia: 'ga',
    hawaii: 'hi',
    idaho: 'id',
    illinois: 'il',
    indiana: 'in',
    iowa: 'ia',
    kansas: 'ks',
    kentucky: 'ky',
    louisiana: 'la',
    maine: 'me',
    maryland: 'md',
    massachusetts: 'ma',
    michigan: 'mi',
    minnesota: 'mn',
    mississippi: 'ms',
    missouri: 'mo',
    montana: 'mt',
    nebraska: 'ne',
    nevada: 'nv',
    'new hampshire': 'nh',
    'new jersey': 'nj',
    'new mexico': 'nm',
    'new york': 'ny',
    'north carolina': 'nc',
    'north dakota': 'nd',
    ohio: 'oh',
    oklahoma: 'ok',
    oregon: 'or',
    pennsylvania: 'pa',
    'rhode island': 'ri',
    'south carolina': 'sc',
    'south dakota': 'sd',
    tennessee: 'tn',
    texas: 'tx',
    utah: 'ut',
    vermont: 'vt',
    virginia: 'va',
    washington: 'wa',
    'west virginia': 'wv',
    wisconsin: 'wi',
    wyoming: 'wy',
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const randomInt = (min, max) => Math.floor(min + Math.random() * (max - min + 1));
const pickRandom = (arr) => arr[Math.floor(Math.random() * arr.length)];

const cleanText = (text) => {
    if (!text) return null;
    const out = String(text).replace(/\s+/g, ' ').trim();
    return out.length ? out : null;
};

const safeJsonParse = (text) => {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
};

const normalizeUrl = (href) => {
    if (!href) return null;
    const trimmed = String(href).trim();
    if (!trimmed) return null;
    if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) return trimmed;
    if (trimmed.startsWith('//')) return `https:${trimmed}`;
    if (trimmed.startsWith('/')) return `${BASE_URL}${trimmed}`;
    return `${BASE_URL}/${trimmed.replace(/^\.?\//, '')}`;
};

const isProbablyBlocked = ({ statusCode, body }) => {
    const s = statusCode || 0;
    const text = (body || '').toLowerCase();
    if ([401, 403, 429, 503].includes(s)) return true;
    if (text.includes('attention required') && text.includes('cloudflare')) return true;
    if (text.includes('sorry, you have been blocked')) return true;
    if (text.includes('cf-ray') && text.includes('cloudflare')) return true;
    return false;
};

const mergeSetCookie = (jar, setCookieHeader) => {
    if (!setCookieHeader) return;
    const cookies = Array.isArray(setCookieHeader) ? setCookieHeader : [setCookieHeader];
    for (const cookieLine of cookies) {
        if (!cookieLine) continue;
        const firstPart = String(cookieLine).split(';')[0];
        const idx = firstPart.indexOf('=');
        if (idx <= 0) continue;
        const name = firstPart.slice(0, idx).trim();
        const value = firstPart.slice(idx + 1).trim();
        if (!name) continue;
        jar[name] = value;
    }
};

const cookieHeaderFromJar = (jar) =>
    Object.entries(jar)
        .filter(([k, v]) => k && v != null)
        .map(([k, v]) => `${k}=${v}`)
        .join('; ');

const getSpecialtySlug = (specialty) => {
    if (!specialty) return 'doctors';
    const normalized = String(specialty).toLowerCase().trim().replace(/-/g, ' ');
    return (
        SPECIALTY_SLUGS[normalized] ||
        String(specialty)
            .toLowerCase()
            .trim()
            .replace(/\s+/g, '-')
            .replace(/[^a-z0-9-]/g, '')
    );
};

const parseLocation = (location) => {
    if (!location) return null;
    const normalized = String(location).toLowerCase().trim();

    if (normalized.includes(',')) {
        const [cityPart, statePart] = normalized.split(',').map((s) => s.trim());
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
            const city = parts
                .slice(0, -1)
                .join('-')
                .replace(/[^a-z0-9-]/g, '');
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

const toPlaywrightProxy = (proxyUrl) => {
    if (!proxyUrl) return undefined;
    try {
        const u = new URL(proxyUrl);
        return {
            server: `${u.protocol}//${u.hostname}:${u.port}`,
            username: u.username ? decodeURIComponent(u.username) : undefined,
            password: u.password ? decodeURIComponent(u.password) : undefined,
        };
    } catch {
        return undefined;
    }
};

// ============================================================================
// EXTRACTION (HTML + JSON-LD)
// ============================================================================

const extractJsonLd = ($) => {
    let data = null;
    $('script[type="application/ld+json"]').each((_, el) => {
        const raw = $(el).text() || $(el).html();
        const parsed = raw ? safeJsonParse(raw) : null;
        const items = parsed ? (Array.isArray(parsed) ? parsed : [parsed]) : [];

        for (const item of items) {
            const type = item?.['@type'];
            if (!type) continue;
            const types = Array.isArray(type) ? type : [type];
            const matches = types.some((t) => ['MedicalBusiness', 'Physician', 'Person', 'LocalBusiness'].includes(t));
            if (!matches) continue;

            const address = item.address && typeof item.address === 'object' ? item.address : null;
            data = {
                name: item.name || null,
                specialty: item.medicalSpecialty?.name || item.specialty || null,
                bio: item.description || null,
                phone: item.telephone || null,
                email: item.email || null,
                image: item.image?.url || item.image || null,
                rating: item.aggregateRating?.ratingValue ?? null,
                reviewCount: item.aggregateRating?.reviewCount ?? null,
                address: address
                    ? {
                          street: address.streetAddress || null,
                          city: address.addressLocality || null,
                          state: address.addressRegion || null,
                          zip: address.postalCode || null,
                      }
                    : null,
            };
            break;
        }
    });
    return data;
};

const extractDoctorFromHtml = ($) => {
    const name =
        cleanText($('h1').first().text()) ||
        cleanText($('[data-testid*="name"], [class*="Name"]').first().text()) ||
        null;

    const phone = cleanText($('a[href^="tel:"]').first().text()) || null;
    const email =
        $('a[href^="mailto:"]').attr('href')?.replace(/^mailto:/i, '').trim() ||
        cleanText($('[data-testid*="email"]').first().text()) ||
        null;
    const website =
        $('a[href*="http"]')
            .filter((_, a) => ($(a).text() || '').toLowerCase().includes('website'))
            .attr('href') || null;

    const specialty =
        cleanText($('[class*="specialty"]').first().text()) ||
        cleanText($('[data-testid*="specialty"]').first().text()) ||
        null;

    const ratingText = cleanText($('[class*="rating"]').first().text()) || '';
    const ratingMatch = ratingText.match(/(\d+(\.\d+)?)/);
    const rating = ratingMatch ? Number(ratingMatch[1]) : null;

    const bio =
        cleanText($('[class*="bio"], [class*="about"], [data-testid*="bio"]').first().text()) ||
        cleanText($('meta[name="description"]').attr('content')) ||
        null;

    const image = $('img[class*="photo"], img[class*="profile"], img[alt*="Dr"]').attr('src') || null;

    return {
        name,
        specialty,
        phone,
        email,
        website,
        rating,
        bio,
        image,
    };
};

const extractDoctorsFromListingHtml = ($) => {
    const doctors = [];
    const seen = new Set();

    const allowedPath = /\/(doctors|dentists|podiatrists|optometrists|chiropractors)\/[^?#]+/i;
    const excluded = /(write-review|claim|insurance|credentials|video|office-locations|reviews)/i;

    $('a[href]').each((_, a) => {
        const href = $(a).attr('href');
        if (!href) return;
        if (!allowedPath.test(href) || excluded.test(href)) return;

        const url = normalizeUrl(href);
        if (!url || seen.has(url)) return;
        seen.add(url);

        const $card = $(a).closest('article, li, section, div');
        const nameRaw =
            cleanText($(a).attr('aria-label')) ||
            cleanText($(a).text()) ||
            cleanText($card.find('h2, h3, h4, [class*="name"]').first().text());

        if (!nameRaw || nameRaw.length < 3) return;
        if (/view|more|see/i.test(nameRaw)) return;

        const specialty = cleanText($card.find('[class*="specialty"]').first().text());
        const location = cleanText($card.find('[class*="location"], [class*="address"]').first().text());
        const ratingText = cleanText($card.find('[class*="rating"]').first().text()) || '';
        const ratingMatch = ratingText.match(/(\d+(\.\d+)?)/);

        doctors.push({
            url,
            name: nameRaw,
            specialty,
            location,
            rating: ratingMatch ? Number(ratingMatch[1]) : null,
        });
    });

    return doctors;
};

// ============================================================================
// EXTRACTION (JSON - Next.js `_next/data/...`)
// ============================================================================

const extractNextDataFromHtml = (html) => {
    if (!html) return null;
    const match = String(html).match(/<script[^>]+id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
    if (!match?.[1]) return null;
    return safeJsonParse(match[1].trim());
};

const buildNextDataUrl = ({ buildId, pageUrl }) => {
    if (!buildId || !pageUrl) return null;
    const u = new URL(pageUrl);
    let pathname = u.pathname;
    if (pathname.endsWith('/')) pathname = pathname.slice(0, -1);
    if (!pathname) pathname = '/';
    if (pathname === '/') pathname = '/index';
    const url = new URL(`${u.origin}/_next/data/${buildId}${pathname}.json`);
    url.search = u.search;
    return url.toString();
};

const walkJson = (root, onObject) => {
    const seen = new Set();
    const stack = [root];
    while (stack.length) {
        const cur = stack.pop();
        if (!cur || typeof cur !== 'object') continue;
        if (seen.has(cur)) continue;
        seen.add(cur);
        if (Array.isArray(cur)) {
            for (const item of cur) stack.push(item);
            continue;
        }
        onObject(cur);
        for (const v of Object.values(cur)) stack.push(v);
    }
};

const normalizeDoctorFromJson = (item) => {
    if (!item || typeof item !== 'object') return null;
    const rawUrl =
        item.profileUrl ||
        item.profile_url ||
        item.url ||
        item.seoUrl ||
        item.seo_url ||
        item.canonicalUrl ||
        item.canonical_url ||
        null;
    const url = normalizeUrl(rawUrl);
    if (!url) return null;

    const name = cleanText(item.name || item.fullName || item.displayName || item.providerName || item.title);
    const specialty =
        cleanText(item.specialty) ||
        cleanText(item.primarySpecialty) ||
        cleanText(Array.isArray(item.specialties) ? item.specialties?.[0] : null) ||
        cleanText(item.medicalSpecialty?.name) ||
        null;

    const city = cleanText(item.city || item.address?.city || item.addressLocality);
    const state = cleanText(item.state || item.address?.state || item.addressRegion);
    const location = city && state ? `${city}, ${state}` : cleanText(item.location || item.practiceLocation || null);

    const rating =
        typeof item.rating === 'number'
            ? item.rating
            : typeof item.averageRating === 'number'
              ? item.averageRating
              : item.aggregateRating?.ratingValue != null
                ? Number(item.aggregateRating.ratingValue)
                : null;

    const reviewCount =
        item.reviewCount != null
            ? Number(item.reviewCount)
            : item.reviews != null
              ? Number(item.reviews)
              : item.aggregateRating?.reviewCount != null
                ? Number(item.aggregateRating.reviewCount)
                : null;

    const doctorId = cleanText(item.id || item.providerId || item.provider_id || item.doctorId || item.doctor_id || null);

    return {
        url,
        doctorId,
        name,
        specialty,
        location,
        rating: Number.isFinite(rating) ? rating : null,
        reviewCount: Number.isFinite(reviewCount) ? reviewCount : null,
    };
};

const extractDoctorsFromNextData = (nextDataJson) => {
    const out = [];
    const seen = new Set();
    const likelyListKeys = ['providers', 'results', 'items', 'profiles', 'doctors', 'physicians'];

    const considerArray = (arr) => {
        if (!Array.isArray(arr) || !arr.length) return;
        for (const item of arr.slice(0, 30)) {
            const normalized = normalizeDoctorFromJson(item);
            if (!normalized?.url) continue;
            if (seen.has(normalized.url)) continue;
            seen.add(normalized.url);
            out.push(normalized);
        }
    };

    walkJson(nextDataJson, (obj) => {
        for (const [k, v] of Object.entries(obj)) {
            const key = k.toLowerCase();
            if (!likelyListKeys.some((x) => key.includes(x))) continue;
            considerArray(v);
        }
    });

    return out;
};

const extractBestProfileObjectFromJson = (root) => {
    let best = null;
    let bestScore = 0;

    const scoreObject = (obj) => {
        const name = obj?.name || obj?.fullName || obj?.displayName;
        if (typeof name !== 'string' || name.trim().length < 3) return 0;
        let score = 1;
        if (obj.telephone || obj.phone || obj.phoneNumber) score += 2;
        if (obj.address || obj.locations || obj.location) score += 1;
        if (obj.bio || obj.description || obj.about) score += 1;
        if (obj.aggregateRating || obj.rating || obj.averageRating) score += 1;
        if (obj.specialty || obj.specialties || obj.medicalSpecialty) score += 1;
        return score;
    };

    walkJson(root, (obj) => {
        const score = scoreObject(obj);
        if (score > bestScore) {
            bestScore = score;
            best = obj;
        }
    });

    return best;
};

const normalizeDoctorDetailsFromJson = (obj) => {
    if (!obj || typeof obj !== 'object') return null;
    const addressObj = obj.address && typeof obj.address === 'object' ? obj.address : null;
    const city = cleanText(addressObj?.city || addressObj?.addressLocality || obj.city);
    const state = cleanText(addressObj?.state || addressObj?.addressRegion || obj.state);

    const specialties = Array.isArray(obj.specialties) ? obj.specialties.map(cleanText).filter(Boolean) : [];
    const specialty =
        cleanText(obj.specialty) ||
        cleanText(obj.primarySpecialty) ||
        cleanText(obj.medicalSpecialty?.name) ||
        specialties[0] ||
        null;

    const rating =
        typeof obj.rating === 'number'
            ? obj.rating
            : typeof obj.averageRating === 'number'
              ? obj.averageRating
              : obj.aggregateRating?.ratingValue != null
                ? Number(obj.aggregateRating.ratingValue)
                : null;

    const reviewCount =
        obj.reviewCount != null
            ? Number(obj.reviewCount)
            : obj.reviews != null
              ? Number(obj.reviews)
              : obj.aggregateRating?.reviewCount != null
                ? Number(obj.aggregateRating.reviewCount)
                : null;

    return {
        name: cleanText(obj.name || obj.fullName || obj.displayName) || null,
        doctorId: cleanText(obj.id || obj.providerId || obj.doctorId || null) || null,
        specialty,
        specialties: specialties.length ? specialties : null,
        bio: cleanText(obj.bio || obj.description || obj.about || null),
        phone: cleanText(obj.telephone || obj.phone || obj.phoneNumber || null),
        email: cleanText(obj.email || null),
        website: cleanText(obj.website || obj.url || null),
        rating: Number.isFinite(rating) ? rating : null,
        reviewCount: Number.isFinite(reviewCount) ? reviewCount : null,
        address:
            city || state
                ? {
                      street: cleanText(addressObj?.street || addressObj?.streetAddress || null),
                      city,
                      state,
                      zip: cleanText(addressObj?.zip || addressObj?.postalCode || null),
                  }
                : null,
        location: city && state ? `${city}, ${state}` : null,
        image: cleanText(obj.image?.url || obj.image || null),
        education: cleanText(obj.education || null),
        certifications: Array.isArray(obj.certifications) ? obj.certifications.map(cleanText).filter(Boolean) : null,
        accepted_insurance: Array.isArray(obj.acceptedInsurance)
            ? obj.acceptedInsurance.map(cleanText).filter(Boolean)
            : null,
    };
};

// ============================================================================
// HTTP CLIENT (got-scraping) + Playwright bootstrap (only if blocked)
// ============================================================================

const createHttpContext = ({ proxyConfiguration, persistState }) => {
    const state = persistState || { buildId: null, cookieJar: {}, userAgent: pickRandom(USER_AGENTS) };
    if (!state.cookieJar || typeof state.cookieJar !== 'object') state.cookieJar = {};
    if (!state.userAgent) state.userAgent = pickRandom(USER_AGENTS);
    if (!state.sessionId) state.sessionId = `vitals_${Date.now()}_${randomInt(1000, 9999)}`;

    const rotateSession = () => {
        state.sessionId = `vitals_${Date.now()}_${randomInt(1000, 9999)}`;
        state.cookieJar = {};
        state.userAgent = pickRandom(USER_AGENTS);
    };

    const getProxyUrl = () => {
        if (!proxyConfiguration?.newUrl) return undefined;
        try {
            return proxyConfiguration.newUrl(state.sessionId);
        } catch {
            return proxyConfiguration.newUrl();
        }
    };

    const fetch = async ({ url, responseType = 'text', headers = {}, maxRetries = 3 }) => {
        let lastErr = null;
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            const proxyUrl = getProxyUrl();
            const cookieHeader = cookieHeaderFromJar(state.cookieJar);
            const ua = state.userAgent || pickRandom(USER_AGENTS);
            const mergedHeaders = {
                accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                pragma: 'no-cache',
                'user-agent': ua,
                ...(cookieHeader ? { cookie: cookieHeader } : {}),
                ...headers,
            };

            try {
                const res = await gotScraping({
                    url,
                    method: 'GET',
                    responseType,
                    proxyUrl,
                    headers: mergedHeaders,
                    timeout: { request: 45000 },
                    http2: true,
                    throwHttpErrors: false,
                    followRedirect: true,
                });

                mergeSetCookie(state.cookieJar, res.headers?.['set-cookie']);
                const body = res.body;
                if (isProbablyBlocked({ statusCode: res.statusCode, body })) {
                    lastErr = new Error(`Blocked (${res.statusCode})`);
                    if (attempt === Math.ceil(maxRetries / 2)) rotateSession();
                    await sleep(randomInt(800, 1600));
                    continue;
                }

                return { statusCode: res.statusCode, headers: res.headers, body, proxyUrlUsed: proxyUrl };
            } catch (err) {
                lastErr = err;
                if (attempt === Math.ceil(maxRetries / 2)) rotateSession();
                await sleep(randomInt(800, 1600));
            }
        }
        throw lastErr || new Error('Request failed');
    };

    return { state, fetch, getProxyUrl };
};

const bootstrapWithPlaywright = async ({ url, httpContext, proxyUrl, hardTimeoutMs = 90000 }) => {
    const ua = pickRandom(USER_AGENTS);
    const proxy = toPlaywrightProxy(proxyUrl);
    const start = Date.now();

    const browser = await firefox.launch({
        headless: true,
        proxy,
        args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--disable-extensions'],
    });

    try {
        const context = await browser.newContext({
            userAgent: ua,
            viewport: { width: 1920 + randomInt(-40, 40), height: 1080 + randomInt(-30, 30) },
            locale: 'en-US',
        });

        await context.addInitScript(() => {
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        });

        const page = await context.newPage();
        await page.route('**/*', (route) => {
            const type = route.request().resourceType();
            const rUrl = route.request().url();
            if (['image', 'font', 'media'].includes(type)) return route.abort();
            if (rUrl.includes('googletagmanager') || rUrl.includes('google-analytics') || rUrl.includes('doubleclick')) return route.abort();
            return route.continue();
        });

        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await page.waitForTimeout(2000);

        while (Date.now() - start < hardTimeoutMs) {
            const content = await page.content();
            if (!isProbablyBlocked({ statusCode: 200, body: content })) break;
            await page.waitForTimeout(1500);
        }

        const cookies = await context.cookies();
        for (const c of cookies) httpContext.state.cookieJar[c.name] = c.value;
        httpContext.state.userAgent = await page.evaluate(() => navigator.userAgent);

        const html = await page.content();
        const nextData = extractNextDataFromHtml(html);
        if (nextData?.buildId) httpContext.state.buildId = nextData.buildId;
        return { html, buildId: httpContext.state.buildId, cookiesCount: cookies.length };
    } finally {
        await browser.close();
    }
};

await Actor.init();
try {
    const input = (await Actor.getInput()) || {};
    const {
        specialty = 'Cardiovascular Disease',
        location = 'New York, NY',
        startUrl,
        collectDetails = true,
        results_wanted: resultsWantedRaw = 50,
        max_pages: maxPagesRaw = 5,
        maxConcurrency: maxConcurrencyRaw = 5,
        proxyConfiguration: proxyConfig,
    } = input;

    const resultsWanted = Number.isFinite(+resultsWantedRaw) ? Math.max(1, +resultsWantedRaw) : 50;
    const maxPages = Number.isFinite(+maxPagesRaw) ? Math.max(1, +maxPagesRaw) : 5;
    const maxConcurrency = Number.isFinite(+maxConcurrencyRaw) ? Math.min(10, Math.max(1, +maxConcurrencyRaw)) : 5;

    const proxyConfiguration = proxyConfig
        ? await Actor.createProxyConfiguration(proxyConfig)
        : await Actor.createProxyConfiguration({ useApifyProxy: true });

    const kvStore = await KeyValueStore.open();
    const persisted = (await kvStore.getValue(KV_KEY)) || {};
    const httpContext = createHttpContext({ proxyConfiguration, persistState: persisted });

    const startTime = Date.now();
    const MAX_RUNTIME_MS = 4.5 * 60 * 1000;
    const stats = {
        listingPages: 0,
        listingDoctors: 0,
        detailPages: 0,
        jsonPages: 0,
        htmlPages: 0,
        playwrightBootstraps: 0,
        blocked: 0,
        errors: 0,
        saved: 0,
    };
    const isTimedOut = () => Date.now() - startTime > MAX_RUNTIME_MS;

    log.info('Vitals.com Physician Scraper');
    log.info(`Search: specialty="${specialty}" slug="${getSpecialtySlug(specialty)}" location="${location}"`);
    log.info(
        `Target: ${resultsWanted} results, maxPages=${maxPages}, collectDetails=${collectDetails}, concurrency=${maxConcurrency}`,
    );

    const listingUrls = [];
    if (startUrl) {
        listingUrls.push(String(startUrl));
    } else {
        for (let page = 1; page <= maxPages; page++) listingUrls.push(buildListingUrl({ specialty, location, page }));
    }

    const listingDoctors = [];
    const seenProfileUrls = new Set();

    const ensureBuildId = async (seedUrl) => {
        if (httpContext.state.buildId) return httpContext.state.buildId;

        let html = null;
        try {
            const res = await httpContext.fetch({ url: seedUrl, responseType: 'text', maxRetries: 2 });
            html = res.body;
        } catch (err) {
            stats.blocked++;
            log.warning(`Seed HTML fetch blocked/failed, bootstrapping with browser: ${err.message}`);
        }

        if (html) {
            const nextData = extractNextDataFromHtml(html);
            if (nextData?.buildId) {
                httpContext.state.buildId = nextData.buildId;
                return httpContext.state.buildId;
            }
        }

        stats.playwrightBootstraps++;
        const proxyUrl = httpContext.getProxyUrl();
        const boot = await bootstrapWithPlaywright({ url: seedUrl, httpContext, proxyUrl });
        log.info(`Bootstrap: cookies=${boot.cookiesCount} buildId=${boot.buildId || 'n/a'}`);
        return httpContext.state.buildId;
    };

    const fetchListingDoctors = async (url) => {
        if (isTimedOut()) return [];

        const buildId = await ensureBuildId(url);
        const nextUrl = buildId ? buildNextDataUrl({ buildId, pageUrl: url }) : null;

        if (nextUrl) {
            try {
                const res = await httpContext.fetch({
                    url: nextUrl,
                    responseType: 'text',
                    headers: { accept: 'application/json,*/*;q=0.8' },
                    maxRetries: 3,
                });
                const json = typeof res.body === 'string' ? safeJsonParse(res.body) : res.body;
                const docs = json ? extractDoctorsFromNextData(json) : [];
                if (docs.length) {
                    stats.jsonPages++;
                    return docs;
                }
            } catch (err) {
                stats.blocked++;
                log.debug(`Listing JSON failed: ${err.message}`);
            }
        }

        try {
            const res = await httpContext.fetch({ url, responseType: 'text', maxRetries: 3 });
            const nextData = extractNextDataFromHtml(res.body);
            if (nextData?.buildId) httpContext.state.buildId = nextData.buildId;
            const $ = cheerioLoad(res.body);
            const docs = extractDoctorsFromListingHtml($);
            if (docs.length) {
                stats.htmlPages++;
                return docs;
            }
        } catch (err) {
            stats.blocked++;
            log.warning(`Listing HTML failed: ${url} (${err.message})`);
        }

        // 3) Browser fallback (only if JSON+HTML failed) - ensures we can handle client-rendered listings.
        try {
            stats.playwrightBootstraps++;
            const proxyUrl = httpContext.getProxyUrl();
            const boot = await bootstrapWithPlaywright({ url, httpContext, proxyUrl, hardTimeoutMs: 60000 });
            const $ = cheerioLoad(boot.html);
            const docs = extractDoctorsFromListingHtml($);
            if (docs.length) {
                stats.htmlPages++;
                return docs;
            }
        } catch (err) {
            stats.errors++;
            log.warning(`Listing Playwright fallback failed: ${url} (${err.message})`);
        }

        return [];
    };

    for (const url of listingUrls) {
        if (isTimedOut() || listingDoctors.length >= resultsWanted) break;

        stats.listingPages++;
        log.info(`Listing: ${url}`);
        const docs = await fetchListingDoctors(url);
        stats.listingDoctors += docs.length;

        for (const d of docs) {
            if (!d?.url) continue;
            if (seenProfileUrls.has(d.url)) continue;
            seenProfileUrls.add(d.url);
            listingDoctors.push(d);
            if (listingDoctors.length >= resultsWanted) break;
        }

        await sleep(randomInt(200, 600));
    }

    if (!listingDoctors.length) {
        await kvStore.setValue(KV_KEY, httpContext.state);
        await Actor.fail(
            'No profiles found. Likely blocked by Cloudflare; try Apify Proxy with residential IPs and lower concurrency.',
        );
    }

    const processOneDetail = async (seed) => {
        if (isTimedOut()) return;
        if (stats.saved >= resultsWanted) return;
        stats.detailPages++;

        const url = seed.url;
        const buildId = httpContext.state.buildId;
        const nextUrl = buildId ? buildNextDataUrl({ buildId, pageUrl: url }) : null;

        if (nextUrl) {
            try {
                const res = await httpContext.fetch({
                    url: nextUrl,
                    responseType: 'text',
                    headers: { accept: 'application/json,*/*;q=0.8' },
                    maxRetries: 3,
                });
                const json = typeof res.body === 'string' ? safeJsonParse(res.body) : res.body;
                if (json) {
                    const bestObj = extractBestProfileObjectFromJson(json);
                    const details = normalizeDoctorDetailsFromJson(bestObj);
                    if (details?.name || details?.phone || details?.address) {
                        stats.jsonPages++;
                        const record = {
                            id: url,
                            doctorId: details.doctorId || seed.doctorId || null,
                            name: details.name || seed.name || null,
                            specialty: details.specialty || seed.specialty || getSpecialtySlug(specialty),
                            specialties: details.specialties || null,
                            location: details.location || seed.location || location || null,
                            phone: details.phone || null,
                            email: details.email || null,
                            website: details.website || null,
                            rating: details.rating ?? seed.rating ?? null,
                            reviews: details.reviewCount ?? seed.reviewCount ?? null,
                            education: details.education || null,
                            certifications: details.certifications || null,
                            accepted_insurance: details.accepted_insurance || null,
                            bio: details.bio || null,
                            image: details.image || null,
                            address: details.address || null,
                            url,
                            source: 'json',
                            fetched_at: new Date().toISOString(),
                        };
                        await Dataset.pushData(record);
                        stats.saved++;
                        return;
                    }
                }
            } catch (err) {
                stats.blocked++;
                log.debug(`Detail JSON failed: ${err.message}`);
            }
        }

        try {
            const res = await httpContext.fetch({ url, responseType: 'text', maxRetries: 3 });
            const nextData = extractNextDataFromHtml(res.body);
            if (nextData?.buildId) httpContext.state.buildId = nextData.buildId;
            const $ = cheerioLoad(res.body);
            const jsonLd = extractJsonLd($);
            const html = extractDoctorFromHtml($);
            stats.htmlPages++;

            const record = {
                id: url,
                doctorId: seed.doctorId || null,
                name: jsonLd?.name || html.name || seed.name || null,
                specialty: jsonLd?.specialty || html.specialty || seed.specialty || getSpecialtySlug(specialty),
                location:
                    (jsonLd?.address?.city && jsonLd?.address?.state ? `${jsonLd.address.city}, ${jsonLd.address.state}` : null) ||
                    seed.location ||
                    location ||
                    null,
                phone: jsonLd?.phone || html.phone || null,
                email: jsonLd?.email || html.email || null,
                website: html.website || null,
                rating: jsonLd?.rating != null ? Number(jsonLd.rating) : html.rating ?? seed.rating ?? null,
                reviews: jsonLd?.reviewCount != null ? Number(jsonLd.reviewCount) : seed.reviewCount ?? null,
                bio: jsonLd?.bio || html.bio || null,
                image: jsonLd?.image || html.image || null,
                address: jsonLd?.address || null,
                url,
                source: jsonLd ? 'json-ld' : 'html',
                fetched_at: new Date().toISOString(),
            };

            await Dataset.pushData(record);
            stats.saved++;
        } catch (err) {
            stats.errors++;
            log.warning(`Detail HTML failed: ${url} (${err.message})`);

            // 3) Browser fallback only if HTTP is blocked/failed.
            try {
                stats.playwrightBootstraps++;
                const proxyUrl = httpContext.getProxyUrl();
                const boot = await bootstrapWithPlaywright({ url, httpContext, proxyUrl, hardTimeoutMs: 60000 });
                const nextData = extractNextDataFromHtml(boot.html);
                if (nextData?.buildId) httpContext.state.buildId = nextData.buildId;
                const $ = cheerioLoad(boot.html);
                const jsonLd = extractJsonLd($);
                const html = extractDoctorFromHtml($);

                const record = {
                    id: url,
                    doctorId: seed.doctorId || null,
                    name: jsonLd?.name || html.name || seed.name || null,
                    specialty: jsonLd?.specialty || html.specialty || seed.specialty || getSpecialtySlug(specialty),
                    location:
                        (jsonLd?.address?.city && jsonLd?.address?.state
                            ? `${jsonLd.address.city}, ${jsonLd.address.state}`
                            : null) ||
                        seed.location ||
                        location ||
                        null,
                    phone: jsonLd?.phone || html.phone || null,
                    email: jsonLd?.email || html.email || null,
                    website: html.website || null,
                    rating: jsonLd?.rating != null ? Number(jsonLd.rating) : html.rating ?? seed.rating ?? null,
                    reviews: jsonLd?.reviewCount != null ? Number(jsonLd.reviewCount) : seed.reviewCount ?? null,
                    bio: jsonLd?.bio || html.bio || null,
                    image: jsonLd?.image || html.image || null,
                    address: jsonLd?.address || null,
                    url,
                    source: jsonLd ? 'json-ld+playwright' : 'html+playwright',
                    fetched_at: new Date().toISOString(),
                };

                await Dataset.pushData(record);
                stats.saved++;
            } catch (pwErr) {
                stats.errors++;
                log.warning(`Detail Playwright fallback failed: ${url} (${pwErr.message})`);
            }
        }
    };

    if (!collectDetails) {
        for (const d of listingDoctors.slice(0, resultsWanted)) {
            await Dataset.pushData({ ...d, id: d.url, source: 'listing', fetched_at: new Date().toISOString() });
            stats.saved++;
            if (stats.saved >= resultsWanted) break;
        }
    } else {
        const pending = listingDoctors.slice(0, resultsWanted);
        const inFlight = new Set();

        while (pending.length && stats.saved < resultsWanted && !isTimedOut()) {
            while (inFlight.size < maxConcurrency && pending.length && stats.saved < resultsWanted && !isTimedOut()) {
                const item = pending.shift();
                let p;
                p = (async () => processOneDetail(item))().finally(() => inFlight.delete(p));
                inFlight.add(p);
                await sleep(randomInt(80, 200));
            }
            if (!inFlight.size) break;
            await Promise.race(inFlight);
        }

        await Promise.allSettled([...inFlight]);
    }

    await kvStore.setValue(KV_KEY, httpContext.state);

    const totalTimeSec = (Date.now() - startTime) / 1000;
    const rate = stats.saved > 0 ? (stats.saved / totalTimeSec).toFixed(2) : '0';

    log.info('='.repeat(60));
    log.info('Execution summary');
    log.info(`Listing pages: ${stats.listingPages}`);
    log.info(`Listing doctors found: ${stats.listingDoctors}`);
    log.info(`Detail pages attempted: ${stats.detailPages}`);
    log.info(`Saved: ${stats.saved}/${resultsWanted}`);
    log.info(`JSON pages parsed: ${stats.jsonPages}`);
    log.info(`HTML pages parsed: ${stats.htmlPages}`);
    log.info(`Playwright bootstraps: ${stats.playwrightBootstraps}`);
    log.info(`Errors: ${stats.errors}`);
    log.info(`Runtime: ${totalTimeSec.toFixed(2)}s (${rate} rec/s)`);
    log.info('='.repeat(60));

    if (stats.saved === 0) {
        await Actor.fail(
            'No results scraped. Likely blocked by Cloudflare; try Apify Proxy with residential IPs and reduce concurrency.',
        );
    } else {
        await Actor.setValue('OUTPUT_SUMMARY', {
            search: { specialty, location, startUrl: startUrl || null },
            resultsWanted,
            maxPages,
            collectDetails,
            stats,
            runtimeSeconds: totalTimeSec,
            success: true,
        });
    }
} catch (error) {
    log.error(`CRITICAL: ${error.message}`);
    log.exception(error, 'Actor failed');
    throw error;
} finally {
    await Actor.exit();
}
