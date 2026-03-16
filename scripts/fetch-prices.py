#!/usr/bin/env python3
"""
PMCI Daily Price Fetcher

Fetches live commodity prices with Commodities-API.com as the PRIMARY source,
falling back to free APIs (Metals.Dev, Yahoo Finance, EIA, BLS) when needed.

Outputs data/prices.json.

Primary API:
  - Commodities-API.com (WTI, Brent, zinc, copper, aluminum, nickel, etc.)

Fallback APIs:
  - Metals.Dev     (LME zinc, copper)         â free tier
  - Yahoo Finance  (WTI crude oil)             â free, real-time data
  - EIA Open Data  (WTI crude oil)             â free with API key
  - BLS Public Data (PPI, ECI)                 â free, no key required

Proxy pricing:
  - Petroleum-derived chemicals track WTI crude oil
  - Metal-derived chemicals track LME zinc or copper
  - Stable commodities apply drift from last known price
"""

import json
import os
import sys
from datetime import datetime, date
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package not installed. Run: pip install requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COMMODITIES_API_KEY = os.environ.get("COMMODITIES_API_KEY", "")
METALS_DEV_API_KEY = os.environ.get("METALS_DEV_API_KEY", "")
EIA_API_KEY = os.environ.get("EIA_API_KEY", "")

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "prices.json"

# Current hardcoded prices from index.html (set during Hormuz crisis, early Mar 2026)
# These serve as the baseline for proxy calculations
HARDCODED = {
    "1":  {"price": 4250,   "name": "Epoxy Resin (DGEBA)",      "cat": "resin"},
    "2":  {"price": 3350,   "name": "Polyurethane Polyol",       "cat": "resin"},
    "3":  {"price": 2700,   "name": "Alkyd Resin (long oil)",    "cat": "resin_bio"},
    "4":  {"price": 3600,   "name": "Acrylic Copolymer (SPC)",   "cat": "resin"},
    "5":  {"price": 2050,   "name": "Rosin (WW grade)",          "cat": "stable"},
    "6":  {"price": 3150,   "name": "Titanium Dioxide (rutile)", "cat": "energy_linked"},
    "7":  {"price": 3650,   "name": "Zinc Dust (fine)",          "cat": "zinc"},
    "8":  {"price": 13488,  "name": "Cuprous Oxide (Cu2O)",      "cat": "copper"},
    "9":  {"price": 1340,   "name": "Iron Oxide (red/yellow)",   "cat": "stable"},
    "10": {"price": 2482,   "name": "Zinc Phosphate",            "cat": "zinc"},
    "11": {"price": 298,    "name": "Extenders (CaCO3/barytes)", "cat": "stable"},
    "12": {"price": 1430,   "name": "Xylene (mixed)",            "cat": "solvent"},
    "13": {"price": 1820,   "name": "MEK",                       "cat": "solvent"},
    "14": {"price": 1850,   "name": "Butyl Acetate",             "cat": "solvent"},
    "15": {"price": 5400,   "name": "Isocyanate Hardener (HDI)", "cat": "petrochem"},
    "16": {"price": 5100,   "name": "Rheology Modifiers",        "cat": "stable"},
    "17": {"price": 9200,   "name": "Co-Biocides (Zineb/ZPT)",   "cat": "zinc_minor"},
    "18": {"price": 114,    "name": "Steel Drums & Cans",        "cat": "ppi"},
    "19": {"price": 195,    "name": "Freight & Shipping",        "cat": "freight"},
    "20": {"price": 138,    "name": "Industrial Energy",         "cat": "energy"},
    "21": {"price": 103.4,  "name": "Manufacturing Labour",      "cat": "eci"},
}

# ---------------------------------------------------------------------------
# Proxy configuration
# ---------------------------------------------------------------------------
# WTI price when hardcoded crisis prices were set (early Mar 2026 Hormuz crisis)
WTI_CRISIS_BASELINE = 90.90  # USD/bbl

# Correlation coefficients: how much each commodity category tracks crude oil
PROXY_OIL_BETA = {
    "solvent":       0.85,
    "resin":         0.55,
    "resin_bio":     0.30,
    "petrochem":     0.60,
    "energy_linked": 0.65,
    "stable":        0.00,
    "zinc_minor":    0.00,
}

# Metal processing factors
ZINC_DUST_PREMIUM        = 1.30
CUPROUS_OXIDE_CU_CONTENT = 0.888
CUPROUS_PROCESSING       = 1.40
ZINC_PHOSPHATE_FACTOR    = 0.68

# Baselines
WTI_BASE_Q1_2025          = 65.0
LME_ZINC_CRISIS_BASELINE  = 2808
LME_COPPER_CRISIS_BASELINE = 10850

# ---------------------------------------------------------------------------
# Commodities-API.com symbols â PMCI commodity mapping
# ---------------------------------------------------------------------------
# These are the symbols we request from Commodities-API.com
# The API returns prices per unit in USD (rates are inverted: 1/rate = price)
CAPI_SYMBOLS = [
    "WTIOIL",     # WTI Crude Oil (USD/bbl)
    "BRENTOIL",   # Brent Crude Oil (USD/bbl)
    "ZNC",        # LME Zinc (USD/MT)
    "XCU",        # LME Copper (USD/MT)
    "ALU",        # Aluminum (USD/MT)
    "NI",         # Nickel (USD/MT)
    "TIN",        # Tin (USD/MT)
    "LEAD",       # Lead (USD/MT)
]

# ---------------------------------------------------------------------------
# PRIMARY: Commodities-API.com Fetcher
# ---------------------------------------------------------------------------
def fetch_commodities_api():
    """
    Fetch live prices from Commodities-API.com.
    Returns dict with raw prices in USD.

    The API returns rates as 1 USD = X units of commodity.
    So the price per unit = 1 / rate.
    """
    results = {
        "wti": None,
        "brent": None,
        "zinc_mt": None,
        "copper_mt": None,
        "aluminum_mt": None,
        "nickel_mt": None,
        "tin_mt": None,
        "lead_mt": None,
    }

    if not COMMODITIES_API_KEY:
        print("  WARN: COMMODITIES_API_KEY not set, skipping Commodities-API.com")
        return results

    try:
        # Free tier limits symbols per request â batch into groups of 2
        BATCH_SIZE = 2
        all_rates = {}
        for i in range(0, len(CAPI_SYMBOLS), BATCH_SIZE):
            batch = CAPI_SYMBOLS[i:i + BATCH_SIZE]
            symbols_str = ",".join(batch)
            url = (
                f"https://commodities-api.com/api/latest"
                f"?access_key={COMMODITIES_API_KEY}"
                f"&base=USD"
                f"&symbols={symbols_str}"
            )
            print(f"  Fetching Commodities-API.com batch {i//BATCH_SIZE + 1}: {symbols_str}...")
            resp = requests.get(url, timeout=30)
            data = resp.json()

            if not data.get("success"):
                error = data.get("error", {})
                print(f"  WARN: Batch {symbols_str} failed: {error.get('info', error.get('type', 'unknown'))}")
                continue

            batch_rates = data.get("data", {}).get("rates", {})
            all_rates.update(batch_rates)

        rates = all_rates
        if not rates:
            print("  WARN: Commodities-API.com returned no rates")
            return results

        # Convert rates (1/rate = price in USD per unit)
        def to_price(symbol):
            r = rates.get(symbol)
            if r and r > 0:
                return round(1.0 / r, 2)
            return None

        results["wti"] = to_price("WTIOIL")
        results["brent"] = to_price("BRENTOIL")
        results["zinc_mt"] = to_price("ZNC")
        results["copper_mt"] = to_price("XCU")
        results["aluminum_mt"] = to_price("ALU")
        results["nickel_mt"] = to_price("NI")
        results["tin_mt"] = to_price("TIN")
        results["lead_mt"] = to_price("LEAD")

        for key, val in results.items():
            if val is not None:
                print(f"    OK: {key}: ${val}")
            else:
                print(f"    MISS: {key}: not available")

        print(f"  Commodities-API.com: {sum(1 for v in results.values() if v is not None)}/{len(results)} prices fetched")

    except Exception as e:
        print(f"  ERROR fetching Commodities-API.com: {e}")

    return results

# ---------------------------------------------------------------------------
# FALLBACK: Metals.Dev API
# ---------------------------------------------------------------------------
def fetch_metals_dev():
    """Fetch LME zinc and copper from Metals.Dev API (fallback)."""
    results = {"zinc_mt": None, "copper_mt": None}
    if not METALS_DEV_API_KEY:
        print("  WARN: METALS_DEV_API_KEY not set, skipping metals fallback")
        return results
    try:
        url = f"https://api.metals.dev/v1/latest?api_key={METALS_DEV_API_KEY}&currency=USD&unit=mt"
        print(f"  Fetching Metals.Dev (fallback)...")
        resp = requests.get(url, timeout=15)
        data = resp.json()
        if data.get("status") != "success":
            print(f"  WARN: Metals.Dev error: {data.get('status', 'unknown')}")
            return results
        metals = data.get("metals", {})
        if "lme_zinc" in metals and metals["lme_zinc"] is not None:
            results["zinc_mt"] = round(metals["lme_zinc"], 2)
            print(f"    OK: LME Zinc: ${results['zinc_mt']}/MT")
        if "lme_copper" in metals and metals["lme_copper"] is not None:
            results["copper_mt"] = round(metals["lme_copper"], 2)
            print(f"    OK: LME Copper: ${results['copper_mt']}/MT")
    except Exception as e:
        print(f"  ERROR fetching metals fallback: {e}")
    return results


# ---------------------------------------------------------------------------
# FALLBACK: Yahoo Finance (WTI)
# ---------------------------------------------------------------------------
def fetch_yahoo_wti():
    """Fetch latest WTI crude oil futures price from Yahoo Finance (fallback)."""
    try:
        import yfinance as yf
        print(f"  Fetching Yahoo Finance WTI (fallback, yfinance)...")
        ticker = yf.Ticker("CL=F")
        hist = ticker.history(period="5d")
        if not hist.empty:
            wti = round(float(hist["Close"].dropna().iloc[-1]), 2)
            print(f"    OK: WTI ${wti}/bbl")
            return wti
        print(f"  WARN: yfinance returned empty data")
    except ImportError:
        print(f"  WARN: yfinance not installed, trying raw API...")
    except Exception as e:
        print(f"  WARN: yfinance failed: {e}, trying raw API...")

    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/CL=F?range=5d&interval=1d"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        print(f"  Fetching Yahoo Finance WTI (fallback, raw API)...")
        resp = requests.get(url, headers=headers, timeout=15)
        data = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None
        close_prices = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        for price in reversed(close_prices):
            if price is not None:
                wti = round(float(price), 2)
                print(f"    OK: WTI ${wti}/bbl")
                return wti
        return None
    except Exception as e:
        print(f"  ERROR fetching Yahoo Finance fallback: {e}")
        return None


# ---------------------------------------------------------------------------
# FALLBACK: EIA (WTI)
# ---------------------------------------------------------------------------
def fetch_eia_wti():
    """Fetch latest WTI crude oil spot price from EIA API v2 (fallback)."""
    if not EIA_API_KEY:
        print("  WARN: EIA_API_KEY not set, skipping EIA fallback")
        return None
    try:
        url = (
            f"https://api.eia.gov/v2/petroleum/pri/spt/data/"
            f"?api_key={EIA_API_KEY}"
            f"&frequency=daily&data[0]=value"
            f"&facets[product][]=EPCWTI"
            f"&facets[series][]=RWTC"
            f"&sort[0][column]=period&sort[0][direction]=desc"
            f"&length=5"
        )
        print(f"  Fetching EIA WTI crude (fallback)...")
        resp = requests.get(url, timeout=15)
        data = resp.json()
        records = data.get("response", {}).get("data", [])
        if records:
            wti = float(records[0]["value"])
            print(f"    OK: WTI ${wti}/bbl ({records[0]['period']}) from EIA")
            return wti
        else:
            print(f"  WARN: No WTI data returned from EIA")
    except Exception as e:
        print(f"  ERROR fetching EIA fallback: {e}")
    return None


# ---------------------------------------------------------------------------
# BLS (PPI, ECI) â no equivalent on Commodities-API
# ---------------------------------------------------------------------------
def fetch_bls_data():
    """Fetch PPI (Steel) and ECI (Labour) from BLS Public Data API."""
    results = {}
    try:
        url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        payload = {
            "seriesid": ["PCU332---332---", "CIU2020000000000A"],
            "startyear": "2025",
            "endyear": "2026",
        }
        bls_key = os.environ.get("BLS_API_KEY", "")
        if bls_key:
            payload["registrationkey"] = bls_key
        print(f"  Fetching BLS PPI and ECI...")
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            print(f"  WARN: BLS status: {data.get('status')}")
            return results
        for series in data.get("Results", {}).get("series", []):
            sid = series.get("seriesID", "")
            series_data = series.get("data", [])
            if not series_data:
                continue
            latest = series_data[0]
            value = float(latest["value"])
            period = f"{latest['year']}-{latest['period']}"
            if "PCU332" in sid:
                ppi_base = 310.0
                steel_index = round((value / ppi_base) * 100.5, 2)
                results["18"] = steel_index
                print(f"    OK: PPI Fabricated Metal: {value} ({period}) -> Index: {steel_index}")
            elif "CIU2020" in sid:
                eci_base = 155.0
                labour_index = round((value / eci_base) * 99.27, 2)
                results["21"] = labour_index
                print(f"    OK: ECI Manufacturing: {value} ({period}) -> Index: {labour_index}")
    except Exception as e:
        print(f"  ERROR fetching BLS: {e}")
    return results


# ---------------------------------------------------------------------------
# Proxy Price Calculations
# ---------------------------------------------------------------------------
def calc_metal_prices(zinc_mt, copper_mt):
    """Calculate metal-derived commodity prices from LME spot."""
    results = {}
    if zinc_mt is not None:
        results["7"] = round(zinc_mt * ZINC_DUST_PREMIUM, 2)
        results["10"] = round(results["7"] * ZINC_PHOSPHATE_FACTOR, 2)
        zinc_ratio = zinc_mt / LME_ZINC_CRISIS_BASELINE
        biocide_base = HARDCODED["17"]["price"]
        results["17"] = round(biocide_base * (1 + 0.30 * (zinc_ratio - 1)), 2)
        print(f"    Zinc Dust: ${results['7']}/MT, Zinc Phosphate: ${results['10']}/MT")
        print(f"    Co-Biocides (30% zinc proxy): ${results['17']}/MT")
    if copper_mt is not None:
        results["8"] = round(copper_mt * CUPROUS_OXIDE_CU_CONTENT * CUPROUS_PROCESSING, 2)
        print(f"    Cuprous Oxide: ${results['8']}/MT")
    return results


def calc_oil_proxies(wti_price):
    """Calculate proxy prices for petroleum-derived commodities."""
    results = {}
    if wti_price is None:
        return results
    oil_ratio = wti_price / WTI_CRISIS_BASELINE
    print(f"    WTI ratio vs crisis baseline: {oil_ratio:.3f} (${wti_price} / ${WTI_CRISIS_BASELINE})")
    for cid, info in HARDCODED.items():
        cat = info["cat"]
        beta = PROXY_OIL_BETA.get(cat)
        if beta is None or beta == 0:
            continue
        adjustment = 1 + beta * (oil_ratio - 1)
        new_price = round(info["price"] * adjustment, 2)
        results[cid] = new_price
        print(f"    #{cid:>2} {info['name']:<30}: ${info['price']:>8} -> ${new_price:>8} (beta={beta}, adj={adjustment:.3f})")
    return results


def calc_energy_index(wti_price):
    """Convert WTI to energy index."""
    if wti_price is None:
        return {}
    energy_index = round((wti_price / WTI_BASE_Q1_2025) * 100, 2)
    print(f"    Energy Index: {energy_index} (WTI ${wti_price} / base ${WTI_BASE_Q1_2025})")
    return {"20": energy_index}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def load_existing():
    if OUTPUT_PATH.exists():
        try:
            with open(OUTPUT_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def main():
    print(f"PMCI Price Fetcher â {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # Start with hardcoded values
    commodities = {}
    for cid, info in HARDCODED.items():
        commodities[cid] = {
            "price": info["price"],
            "name": info["name"],
            "source": "hardcoded",
            "live": False,
        }

    # -----------------------------------------------------------------------
    # [1/4] PRIMARY: Commodities-API.com
    # -----------------------------------------------------------------------
    print("\n[1/4] PRIMARY â Commodities-API.com...")
    capi = fetch_commodities_api()

    # Extract WTI and metals from Commodities-API
    wti_price = capi.get("wti")
    brent_price = capi.get("brent")
    zinc_mt = capi.get("zinc_mt")
    copper_mt = capi.get("copper_mt")

    wti_source = "commodities-api"
    zinc_source = "commodities-api"
    copper_source = "commodities-api"

    # -----------------------------------------------------------------------
    # [2/4] FALLBACK: Metals (if Commodities-API missed zinc/copper)
    # -----------------------------------------------------------------------
    print("\n[2/4] FALLBACK â Metals (Metals.Dev)...")
    if zinc_mt is None or copper_mt is None:
        print("  Commodities-API missing metals, trying Metals.Dev fallback...")
        metals_fb = fetch_metals_dev()
        if zinc_mt is None and metals_fb["zinc_mt"] is not None:
            zinc_mt = metals_fb["zinc_mt"]
            zinc_source = "metals-dev"
            print(f"    -> Using Metals.Dev zinc: ${zinc_mt}/MT")
        if copper_mt is None and metals_fb["copper_mt"] is not None:
            copper_mt = metals_fb["copper_mt"]
            copper_source = "metals-dev"
            print(f"    -> Using Metals.Dev copper: ${copper_mt}/MT")
    else:
        print("  Commodities-API provided metals, skipping Metals.Dev")

    # -----------------------------------------------------------------------
    # [3/4] FALLBACK: WTI (if Commodities-API missed WTI)
    # -----------------------------------------------------------------------
    print("\n[3/4] FALLBACK â WTI (Yahoo Finance / EIA)...")
    if wti_price is None:
        print("  Commodities-API missing WTI, trying Yahoo Finance fallback...")
        wti_price = fetch_yahoo_wti()
        if wti_price is not None:
            wti_source = "yahoo-finance"
        else:
            print("  Yahoo Finance failed, trying EIA fallback...")
            wti_price = fetch_eia_wti()
            if wti_price is not None:
                wti_source = "eia"
    else:
        print(f"  Commodities-API provided WTI (${wti_price}), skipping fallbacks")
        # Still check EIA for comparison
        eia_wti = fetch_eia_wti()
        if eia_wti is not None and eia_wti > wti_price:
            print(f"  NOTE: EIA WTI (${eia_wti}) higher than CAPI (${wti_price})")

    # -----------------------------------------------------------------------
    # [4/4] BLS (PPI Steel, ECI Labour) â no Commodities-API equivalent
    # -----------------------------------------------------------------------
    print("\n[4/4] BLS (PPI Steel, ECI Labour)...")
    bls = fetch_bls_data()

    # -----------------------------------------------------------------------
    # Compute derived prices
    # -----------------------------------------------------------------------
    print("\n[Proxy] Metal-derived commodities...")
    metal_prices = calc_metal_prices(zinc_mt, copper_mt)

    print("\n[Proxy] Oil-linked commodities...")
    oil_proxies = calc_oil_proxies(wti_price)

    print("\n[Proxy] Energy index...")
    energy = calc_energy_index(wti_price)

    # -----------------------------------------------------------------------
    # Merge results (priority: direct API > metal proxy > oil proxy > hardcoded)
    # -----------------------------------------------------------------------
    # Oil proxies first (lower priority)
    for cid, price in oil_proxies.items():
        commodities[cid]["price"] = price
        commodities[cid]["source"] = f"proxy:wti({wti_source})"
        commodities[cid]["live"] = True

    # Metal proxies (higher priority, overrides oil proxy for zinc/copper items)
    for cid, price in metal_prices.items():
        src = f"lme-zinc({zinc_source})" if cid in ("7", "10", "17") else f"lme-copper({copper_source})"
        commodities[cid]["price"] = price
        commodities[cid]["source"] = src
        commodities[cid]["live"] = True

    # Direct BLS data
    for cid, price in bls.items():
        source = "bls-ppi" if cid == "18" else "bls-eci"
        commodities[cid]["price"] = price
        commodities[cid]["source"] = source
        commodities[cid]["live"] = True

    # Energy index (direct from WTI)
    for cid, price in energy.items():
        commodities[cid]["price"] = price
        commodities[cid]["source"] = f"wti-energy({wti_source})"
        commodities[cid]["live"] = True

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    live_count = sum(1 for c in commodities.values() if c["live"])
    print(f"\n{'=' * 60}")
    print(f"Results: {live_count}/21 commodities updated")
    print(f"Data sources: WTI={wti_source}, Zinc={zinc_source}, Copper={copper_source}")
    for cid in sorted(commodities.keys(), key=int):
        c = commodities[cid]
        tag = "LIVE" if c["live"] else "STATIC"
        print(f"  [{tag:>6}] #{cid:>2} {c['name']:<30} ${c['price']:>10} ({c['source']})")

    # -----------------------------------------------------------------------
    # Write output
    # -----------------------------------------------------------------------
    output = {
        "updated": date.today().isoformat(),
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "live_count": live_count,
        "total_count": 21,
        "wti_price": wti_price,
        "brent_price": brent_price,
        "lme_zinc": zinc_mt,
        "lme_copper": copper_mt,
        "data_sources": {
            "primary": "commodities-api.com",
            "wti_source": wti_source,
            "zinc_source": zinc_source,
            "copper_source": copper_source,
        },
        "commodities": commodities,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nWritten to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
