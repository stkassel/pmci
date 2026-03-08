#!/usr/bin/env python3
"""
PMCI Daily Price Fetcher
Fetches live commodity prices from free APIs and computes proxy prices
for commodities without free data. Outputs data/prices.json.

Free APIs:
  - Metals.Dev (LME zinc, copper) â free tier
  - Yahoo Finance (WTI crude oil) â free, real-time data
  - EIA Open Data (WTI crude oil) â free with API key, used as fallback
  - BLS Public Data (PPI, ECI) â free, no key required

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
METALS_DEV_API_KEY = os.environ.get("METALS_DEV_API_KEY", "")
EIA_API_KEY = os.environ.get("EIA_API_KEY", "")

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "prices.json"

# Current hardcoded prices from index.html (set during Hormuz crisis, early Mar 2026)
# These serve as the baseline for proxy calculations
HARDCODED = {
    "1":  {"price": 4250,   "name": "Epoxy Resin (DGEBA)",        "cat": "resin"},
    "2":  {"price": 3350,   "name": "Polyurethane Polyol",         "cat": "resin"},
    "3":  {"price": 2700,   "name": "Alkyd Resin (long oil)",      "cat": "resin_bio"},
    "4":  {"price": 3600,   "name": "Acrylic Copolymer (SPC)",     "cat": "resin"},
    "5":  {"price": 2050,   "name": "Rosin (WW grade)",            "cat": "stable"},
    "6":  {"price": 3150,   "name": "Titanium Dioxide (rutile)",   "cat": "energy_linked"},
    "7":  {"price": 3650,   "name": "Zinc Dust (fine)",            "cat": "zinc"},
    "8":  {"price": 13488,  "name": "Cuprous Oxide (Cu2O)",        "cat": "copper"},
    "9":  {"price": 1340,   "name": "Iron Oxide (red/yellow)",     "cat": "stable"},
    "10": {"price": 2482,   "name": "Zinc Phosphate",              "cat": "zinc"},
    "11": {"price": 298,    "name": "Extenders (CaCO3/barytes)",   "cat": "stable"},
    "12": {"price": 1430,   "name": "Xylene (mixed)",              "cat": "solvent"},
    "13": {"price": 1820,   "name": "MEK",                         "cat": "solvent"},
    "14": {"price": 1850,   "name": "Butyl Acetate",               "cat": "solvent"},
    "15": {"price": 5400,   "name": "Isocyanate Hardener (HDI)",   "cat": "petrochem"},
    "16": {"price": 5100,   "name": "Rheology Modifiers",          "cat": "stable"},
    "17": {"price": 9200,   "name": "Co-Biocides (Zineb/ZPT)",     "cat": "zinc_minor"},
    "18": {"price": 114,    "name": "Steel Drums & Cans",          "cat": "ppi"},
    "19": {"price": 195,    "name": "Freight & Shipping",          "cat": "freight"},
    "20": {"price": 138,    "name": "Industrial Energy",           "cat": "energy"},
    "21": {"price": 103.4,  "name": "Manufacturing Labour",        "cat": "eci"},
}

# ---------------------------------------------------------------------------
# Proxy configuration
# ---------------------------------------------------------------------------
# WTI price when hardcoded crisis prices were set (early Mar 2026 Hormuz crisis)
WTI_CRISIS_BASELINE = 90.90  # USD/bbl

# Correlation coefficients: how much each commodity category tracks crude oil
# 1.0 = moves 1:1 with oil, 0.5 = moves 50% as much, 0.0 = independent
PROXY_OIL_BETA = {
    "solvent":       0.85,  # Xylene, MEK, BuAc: petroleum-derived, very high correlation
    "resin":         0.55,  # Epoxy, PU, Acrylic: petrochemical feedstock, moderate correlation
    "resin_bio":     0.30,  # Alkyd: partly bio-based (soybean oil), lower oil correlation
    "petrochem":     0.60,  # HDI: petrochemical but more processing, moderate correlation
    "energy_linked": 0.65,  # TiO2: chloride process is energy-intensive
    "stable":        0.00,  # Rosin, Iron Oxide, Extenders, Rheology: minimal oil correlation
    "zinc_minor":    0.00,  # Co-Biocides: zinc input but tracked separately
}

# Zinc/Copper processing factors for direct metal pricing
ZINC_DUST_PREMIUM = 1.30        # LME zinc Ã 1.30 â zinc dust fine
CUPROUS_OXIDE_CU_CONTENT = 0.888
CUPROUS_PROCESSING = 1.40       # Processing premium
ZINC_PHOSPHATE_FACTOR = 0.68    # Tracks ~68% of zinc dust price

# Energy index: WTI baseline for conversion (100 = Q1 2025 avg)
WTI_BASE_Q1_2025 = 65.0

# LME baselines when crisis prices were set (for proxy calc)
LME_ZINC_CRISIS_BASELINE = 2808   # USD/MT (approx LME zinc early Mar 2026)
LME_COPPER_CRISIS_BASELINE = 10850  # USD/MT (approx LME copper early Mar 2026)


# ---------------------------------------------------------------------------
# API Fetchers
# ---------------------------------------------------------------------------
def fetch_metals_dev():
    """Fetch LME zinc and copper from Metals.Dev API."""
    results = {"zinc_mt": None, "copper_mt": None}

    if not METALS_DEV_API_KEY:
        print("  WARN: METALS_DEV_API_KEY not set, skipping metals fetch")
        return results

    try:
        url = f"https://api.metals.dev/v1/latest?api_key={METALS_DEV_API_KEY}&currency=USD&unit=mt"
        print(f"  Fetching Metals.Dev...")
        resp = requests.get(url, timeout=15)
        data = resp.json()

        if data.get("status") != "success":
            print(f"  WARN: Metals.Dev error: {data.get('status', 'unknown')}")
            return results

        metals = data.get("metals", {})

        if "lme_zinc" in metals and metals["lme_zinc"] is not None:
            results["zinc_mt"] = round(metals["lme_zinc"], 2)
            print(f"  OK: LME Zinc: ${results['zinc_mt']}/MT")

        if "lme_copper" in metals and metals["lme_copper"] is not None:
            results["copper_mt"] = round(metals["lme_copper"], 2)
            print(f"  OK: LME Copper: ${results['copper_mt']}/MT")

    except Exception as e:
        print(f"  ERROR fetching metals: {e}")

    return results


def fetch_yahoo_wti():
    """Fetch latest WTI crude oil futures price from Yahoo Finance."""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/CL=F?range=5d&interval=1d"
        print(f"  Fetching Yahoo Finance WTI...")
        resp = requests.get(url, timeout=15)
        data = resp.json()

        result = data.get("chart", {}).get("result", [])
        if not result:
            print(f"  WARN: No Yahoo Finance data returned")
            return None

        close_prices = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])

        # Get most recent non-null closing price
        for price in reversed(close_prices):
            if price is not None:
                wti = float(price)
                print(f"  OK: WTI ${wti}/bbl (from Yahoo Finance)")
                return wti

        print(f"  WARN: No valid WTI price in Yahoo Finance data")
        return None

    except Exception as e:
        print(f"  ERROR fetching Yahoo Finance: {e}")

    return None


def fetch_eia_wti():
    """Fetch latest WTI crude oil spot price from EIA API v2."""
    if not EIA_API_KEY:
        print("  WARN: EIA_API_KEY not set, skipping EIA energy fetch")
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
            print(f"  OK: WTI ${wti}/bbl ({records[0]['period']}) from EIA")
            return wti
        else:
            print(f"  WARN: No WTI data returned from EIA")

    except Exception as e:
        print(f"  ERROR fetching EIA: {e}")

    return None


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
                print(f"  OK: PPI Fabricated Metal: {value} ({period}) â Index: {steel_index}")

            elif "CIU2020" in sid:
                eci_base = 155.0
                labour_index = round((value / eci_base) * 99.27, 2)
                results["21"] = labour_index
                print(f"  OK: ECI Manufacturing: {value} ({period}) â Index: {labour_index}")

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
        # Co-Biocides partially track zinc (30% zinc input)
        zinc_ratio = zinc_mt / LME_ZINC_CRISIS_BASELINE
        biocide_base = HARDCODED["17"]["price"]
        results["17"] = round(biocide_base * (1 + 0.30 * (zinc_ratio - 1)), 2)
        print(f"  Zinc Dust: ${results['7']}/MT, Zinc Phosphate: ${results['10']}/MT")
        print(f"  Co-Biocides (30% zinc proxy): ${results['17']}/MT")

    if copper_mt is not None:
        results["8"] = round(copper_mt * CUPROUS_OXIDE_CU_CONTENT * CUPROUS_PROCESSING, 2)
        print(f"  Cuprous Oxide: ${results['8']}/MT")

    return results


def calc_oil_proxies(wti_price):
    """Calculate proxy prices for petroleum-derived commodities."""
    results = {}

    if wti_price is None:
        return results

    oil_ratio = wti_price / WTI_CRISIS_BASELINE  # e.g., 85/90.90 = 0.935

    print(f"  WTI ratio vs crisis baseline: {oil_ratio:.3f} (${wti_price} / ${WTI_CRISIS_BASELINE})")

    for cid, info in HARDCODED.items():
        cat = info["cat"]
        beta = PROXY_OIL_BETA.get(cat)

        if beta is None or beta == 0:
            continue

        # Proxy formula: new_price = base Ã (1 + beta Ã (oil_ratio - 1))
        # If oil drops 10% and beta=0.85, price drops 8.5%
        adjustment = 1 + beta * (oil_ratio - 1)
        new_price = round(info["price"] * adjustment, 2)
        results[cid] = new_price
        print(f"  #{cid:>2} {info['name']:<30}: ${info['price']:>8} â ${new_price:>8} (beta={beta}, adj={adjustment:.3f})")

    return results


def calc_energy_index(wti_price):
    """Convert WTI to energy index."""
    if wti_price is None:
        return {}
    energy_index = round((wti_price / WTI_BASE_Q1_2025) * 100, 2)
    print(f"  Energy Index: {energy_index} (WTI ${wti_price} / base ${WTI_BASE_Q1_2025})")
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

    # --- Fetch live data ---
    print("\n[1/3] Metals (LME Zinc, Copper)...")
    metals_raw = fetch_metals_dev()

    print("\n[2/3] Energy (WTI Crude)...")
    # Try Yahoo Finance first for more current data, fall back to EIA
    wti_price = fetch_yahoo_wti()
    if wti_price is None:
        print("  Yahoo Finance WTI fetch failed, trying EIA...")
        wti_price = fetch_eia_wti()
    else:
        # If Yahoo succeeded, also try EIA to compare freshness
        eia_wti = fetch_eia_wti()
        if eia_wti is not None and eia_wti > wti_price:
            print(f"  EIA WTI (${eia_wti}) higher than Yahoo (${wti_price}), using EIA")
            wti_price = eia_wti

    print("\n[3/3] BLS (PPI Steel, ECI Labour)...")
    bls = fetch_bls_data()

    # --- Compute derived prices ---
    print("\n[Proxy] Metal-derived commodities...")
    metal_prices = calc_metal_prices(metals_raw["zinc_mt"], metals_raw["copper_mt"])

    print("\n[Proxy] Oil-linked commodities...")
    oil_proxies = calc_oil_proxies(wti_price)

    print("\n[Proxy] Energy index...")
    energy = calc_energy_index(wti_price)

    # --- Merge results (priority: direct API > metal proxy > oil proxy > hardcoded) ---
    # Oil proxies first (lower priority)
    for cid, price in oil_proxies.items():
        commodities[cid]["price"] = price
        commodities[cid]["source"] = "proxy:wti"
        commodities[cid]["live"] = True

    # Metal proxies (higher priority, overrides oil proxy for zinc/copper items)
    for cid, price in metal_prices.items():
        commodities[cid]["price"] = price
        src = "lme-zinc" if cid in ("7", "10", "17") else "lme-copper"
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
        commodities[cid]["source"] = "eia-wti"
        commodities[cid]["live"] = True

    # --- Summary ---
    live_count = sum(1 for c in commodities.values() if c["live"])
    print(f"\n{'=' * 60}")
    print(f"Results: {live_count}/21 commodities updated")
    for cid in sorted(commodities.keys(), key=int):
        c = commodities[cid]
        tag = "LIVE" if c["live"] else "STATIC"
        print(f"  [{tag:>6}] #{cid:>2} {c['name']:<30} ${c['price']:>10} ({c['source']})")

    # --- Write output ---
    output = {
        "updated": date.today().isoformat(),
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "live_count": live_count,
        "total_count": 21,
        "wti_price": wti_price,
        "lme_zinc": metals_raw.get("zinc_mt"),
        "lme_copper": metals_raw.get("copper_mt"),
        "commodities": commodities,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nWritten to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
