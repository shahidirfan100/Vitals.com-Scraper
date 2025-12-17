# Vitals.com Physician Scraper

> **Extract comprehensive physician profiles and ratings from Vitals.com** - America's most trusted healthcare provider directory with over 1 million doctors and specialists.

[![Apify Actor](https://img.shields.io/badge/Apify-Actor-blue)](https://apify.com)
[![Doctor Data](https://img.shields.io/badge/Data-Physicians-green)](https://apify.com)
[![Healthcare](https://img.shields.io/badge/Industry-Healthcare-orange)](https://apify.com)

## Overview

Vitals.com Physician Scraper is a production-ready actor that extracts comprehensive physician and healthcare provider information from the Vitals.com directory. It collects detailed profiles including credentials, specialties, patient ratings, and availability data.

### Primary Use Cases

- **Healthcare Intelligence** - Build comprehensive provider databases and directories
- **Physician Recruitment** - Identify specialists in specific fields and locations
- **Market Research** - Analyze physician distribution and specialization trends
- **Medical Directory Integration** - Power healthcare platforms with fresh provider data
- **Patient Research Tools** - Enable patients to compare and evaluate healthcare providers
- **Insurance Networks** - Verify and update in-network physician databases

## Key Features

### Data Extraction Methods
- **JSON API Priority** - Fast, structured data retrieval using internal APIs
- **HTML Fallback** - Comprehensive parsing when API access is limited
- **JSON-LD Support** - Extract schema.org markup for rich data
- **Multi-field Extraction** - Credentials, ratings, insurance, specialties

### Performance & Reliability
- **Concurrent Processing** - Configurable parallel requests for speed
- **Timeout Safety** - Graceful handling of time constraints
- **Proxy Support** - Built-in Apify Proxy integration
- **Error Recovery** - Automatic fallback mechanisms
- **Rate Limiting** - Respectful request pacing

### Data Quality
- **Duplicate Prevention** - Intelligent deduplication
- **Field Validation** - Clean, normalized data output
- **Comprehensive Profiles** - Complete physician information
- **Real-time Updates** - Current directory data

## Getting Started

### Minimum Input (Quick Search)

```json
{
  "specialty": "Cardiologist",
  "results_wanted": 10
}
```

### Standard Configuration (Full Details)

```json
{
  "specialty": "Cardiovascular Disease",
  "location": "New York",
  "results_wanted": 50,
  "collectDetails": true,
  "maxConcurrency": 3
}
```

### Advanced Configuration (Large-Scale Collection)

```json
{
  "specialty": "Dermatology",
  "location": "California",
  "insurance": "Aetna",
  "results_wanted": 200,
  "max_pages": 5,
  "collectDetails": true,
  "maxConcurrency": 5,
  "proxyConfiguration": {
    "useApifyProxy": true
  }
}
```

## Input Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `specialty` | string | No | Medical specialty or field | `"Cardiologist"`, `"Psychiatry"` |
| `location` | string | No | Geographic location or state | `"New York"`, `"Los Angeles"` |
| `insurance` | string | No | Insurance provider name | `"Blue Cross"`, `"United Health"` |
| `startUrl` | string | No | Direct Vitals.com search URL | `"https://www.vitals.com/doctors?..."` |
| `results_wanted` | integer | No | Target number of physicians (1-500) | `50` |
| `max_pages` | integer | No | Maximum search pages to process | `5` |
| `collectDetails` | boolean | No | Extract full physician profiles | `true` |
| `maxConcurrency` | integer | No | Parallel request limit (1-10) | `3` |
| `proxyConfiguration` | object | No | Proxy settings for reliability | `{"useApifyProxy": true}` |

## Output Data Structure

Each physician record contains:

```json
{
  "id": "unique-physician-id",
  "doctorId": "doctor-profile-id",
  "name": "Dr. John Smith",
  "specialty": "Cardiology",
  "location": "New York, NY",
  "phone": "(212) 555-0123",
  "email": "contact@clinic.com",
  "website": "https://drsmith-cardiology.com",
  "rating": 4.8,
  "reviews": 287,
  "education": "Harvard Medical School, MD",
  "experience": 15,
  "certifications": ["Board Certified - American College of Cardiology"],
  "accepted_insurance": ["Aetna", "Blue Cross", "Cigna"],
  "bio": "Dr. Smith specializes in interventional cardiology...",
  "bio_html": "<p>Dr. Smith specializes in interventional cardiology...</p>",
  "url": "https://www.vitals.com/doctors/dr-john-smith",
  "source": "api",
  "fetched_at": "2024-01-15T10:30:00.000Z"
}
```

### Field Descriptions

- **`id`** - Unique physician identifier
- **`doctorId`** - Profile ID from Vitals.com
- **`name`** - Full physician name
- **`specialty`** - Primary medical specialty
- **`location`** - Office location (city, state)
- **`phone`** - Contact phone number
- **`email`** - Contact email address
- **`website`** - Physician website or practice URL
- **`rating`** - Average patient rating (0-5 scale)
- **`reviews`** - Total number of patient reviews
- **`education`** - Medical education and degrees
- **`experience`** - Years of medical practice
- **`certifications`** - Board certifications and credentials
- **`accepted_insurance`** - Insurance providers accepted
- **`bio`** - Plain text physician biography
- **`bio_html`** - HTML formatted biography
- **`url`** - Direct link to Vitals profile
- **`source`** - Data source (api/html-fallback)
- **`fetched_at`** - Extraction timestamp

## Use Cases & Applications

### Healthcare Intelligence Platforms
Extract physician data to power patient review platforms, healthcare ratings, and provider comparison tools with up-to-date information.

### Recruitment & Staffing
Identify and contact specialists for clinical staffing needs, hospital recruitment, or private practice partnerships.

### Medical Research
Analyze physician distribution, specialization patterns, and geographic availability across the United States.

### Insurance Network Verification
Validate and update in-network physician databases to ensure accurate coverage information for members.

### Patient Safety Applications
Enable patients to research providers, verify credentials, read reviews, and make informed healthcare decisions.

## Performance & Optimization

### Recommended Configurations by Use Case

| Use Case | Specialty | Location | Results | Pages | Time |
|----------|-----------|----------|---------|-------|------|
| Research | Yes | No | 25 | 2 | ~1 min |
| Standard Search | Yes | Yes | 50 | 3 | ~2 min |
| Comprehensive | Yes | Yes | 100 | 5 | ~4 min |
| Large Dataset | Yes | Yes | 250 | 10 | ~8 min |

### Cost Effectiveness

- **Free Tier**: Up to 50 physicians per run
- **Standard Pricing**: Approximately $0.001 per physician profile
- **Proxy Costs**: Additional for premium proxy services (recommended)

### Performance Tips

1. **Start with specialty filtering** - Narrow results to specific medical fields
2. **Use geographic constraints** - Limit to relevant states or regions
3. **Enable proxies** - Apify Proxy improves reliability and speed
4. **Monitor concurrency** - Balance speed with resource usage (3-5 recommended)
5. **Batch large requests** - Split massive collections into multiple runs

## Configuration Examples

### Cardiology Specialists in New York

```json
{
  "specialty": "Cardiology",
  "location": "New York",
  "results_wanted": 30,
  "collectDetails": true
}
```

### Dermatologists Accepting Medicare

```json
{
  "specialty": "Dermatology",
  "location": "Florida",
  "insurance": "Medicare",
  "results_wanted": 40,
  "max_pages": 3
}
```

### Pediatricians - Multi-State Search

```json
{
  "specialty": "Pediatrics",
  "results_wanted": 100,
  "max_pages": 5,
  "collectDetails": true,
  "maxConcurrency": 4
}
```

### Psychiatry with Full Profiles

```json
{
  "specialty": "Psychiatry",
  "location": "California",
  "results_wanted": 75,
  "collectDetails": true,
  "max_pages": 4
}
```

## System Requirements & Coverage

### Geographic Coverage
- **United States Coverage** - All 50 states supported
- **Major Metropolitan Areas** - All major cities included
- **Rural Areas** - Comprehensive rural provider data

### Specialty Coverage
Supports all medical specialties including:
- Cardiology, Neurology, Orthopedics
- Dermatology, Psychiatry, Oncology
- Primary Care, Pediatrics, Obstetrics
- Surgery, Internal Medicine, and more

### Data Attributes
- Complete physician profiles with credentials
- Patient ratings and review aggregation
- Insurance network information
- Educational background and certifications
- Years of experience and practice details

## Troubleshooting

### No Results Returned
- Verify specialty name and spelling
- Check location format and validity
- Try broader search terms
- Ensure internet connectivity

### Timeout Issues
- Reduce `results_wanted` value
- Lower `max_pages` setting
- Decrease `maxConcurrency`
- Enable proxy configuration

### Incomplete Physician Data
- Ensure `collectDetails` is set to `true`
- Check if profiles are publicly available on Vitals.com
- Some physicians may have limited profile information

### API Errors
- Enable `useApifyProxy` for stability
- Check input parameter formatting
- Reduce concurrent requests if rate limited
- Verify Apify account quota availability

## Dataset Output

All extracted physician data is automatically saved to the Apify Dataset in standardized JSON format, ready for:
- Direct API consumption
- Export to CSV/Excel
- Database integration
- Business intelligence tools
- Third-party applications

## Rate Limiting & Ethics

- Respects website robots.txt directives
- Implements intelligent rate limiting
- Configurable request throttling
- Designed for ethical data extraction
- Compliant with Terms of Service

## Support & Documentation

For additional help:
- Review input parameter examples
- Check configuration templates
- Verify specialty and location values
- Enable proxy for enhanced reliability
- Monitor execution logs for diagnostics

## License & Terms

This actor extracts publicly available physician directory information in accordance with Vitals.com Terms of Service and applicable data collection regulations.

---

**Keywords**: physician directory, doctor profiles, healthcare providers, medical specialists, Vitals ratings, patient reviews, provider search, medical credentials, healthcare intelligence, directory scraping
