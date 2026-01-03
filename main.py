import contextlib
import json
from pathlib import Path

import polars as pl

# Soviet/post-Soviet countries with Hebrew names and flag emojis
SOVIET_COUNTRIES = {
    "russia": {"he": "רוסיה", "en": "Russia", "flag": "🇷🇺"},
    "ukraine": {"he": "אוקראינה", "en": "Ukraine", "flag": "🇺🇦"},
    "ussr": {"he": 'בריה"מ (לשעבר)', "en": "Former USSR", "flag": "🚩"},
    "belarus": {"he": "בלארוס", "en": "Belarus", "flag": "🇧🇾"},
    "moldova": {"he": "מולדובה", "en": "Moldova", "flag": "🇲🇩"},
    "uzbekistan": {"he": "אוזבקיסטן", "en": "Uzbekistan", "flag": "🇺🇿"},
    "azerbaijan": {"he": "אזרבייג'ן", "en": "Azerbaijan", "flag": "🇦🇿"},
    "georgia": {"he": "גאורגיה", "en": "Georgia", "flag": "🇬🇪"},
    "kazakhstan": {"he": "קזחסטן", "en": "Kazakhstan", "flag": "🇰🇿"},
    "lithuania": {"he": "ליטא", "en": "Lithuania", "flag": "🇱🇹"},
    "latvia": {"he": "לטביה", "en": "Latvia", "flag": "🇱🇻"},
    "estonia": {"he": "אסטוניה", "en": "Estonia", "flag": "🇪🇪"},
    "tajikistan": {"he": "טג'יקיסטן", "en": "Tajikistan", "flag": "🇹🇯"},
    "turkmenistan": {"he": "טורקמניסטן", "en": "Turkmenistan", "flag": "🇹🇲"},
    "kyrgyzstan": {"he": "קירגיסטן", "en": "Kyrgyzstan", "flag": "🇰🇬"},
    "armenia": {"he": "ארמניה", "en": "Armenia", "flag": "🇦🇲"},
}

GEOJSON_PATH = Path("statistical_areas_2022/statistical_areas.geojson")
TOP_AREAS_PATH = Path("statistical_areas_2022/top_areas.json")
CENSUS_PATH = Path("data/census_stat_areas.csv")

# Color modes that need top areas computed
COLOR_MODES = [
    "soviet_origin_pct", "soviet_birth_pct",
    "russia_origin_pct", "russia_birth_pct",
    "ussr_origin_pct", "ussr_birth_pct",
    "ukraine_origin_pct", "ukraine_birth_pct",
    "uzbekistan_origin_pct", "uzbekistan_birth_pct",
    "belarus_origin_pct", "belarus_birth_pct",
    "moldova_origin_pct", "moldova_birth_pct",
]


def match_soviet_country(country_name):
    """Match a Hebrew country name to a Soviet country key."""
    if not country_name:
        return None
    country_name = str(country_name).strip()
    for key, info in SOVIET_COUNTRIES.items():
        if info["he"] in country_name or country_name in info["he"]:
            return key
    return None


def parse_yishuv_sta(semel, stat_area):
    """Parse settlement code and stat area into YISHUV_STA key."""
    if semel is None or stat_area is None:
        return None
    with contextlib.suppress(ValueError, TypeError):
        semel_int = int(float(semel))
        stat_int = int(float(stat_area))
        return f"{semel_int}{stat_int:04d}"
    return None


def extract_country_pct(row, country_col, pct_col, suffix):
    """Extract country percentage from a row if valid."""
    if country_col >= len(row) or pct_col >= len(row):
        return None, None
    country_name = row[country_col]
    pct = row[pct_col]
    country_key = match_soviet_country(country_name)
    if country_key and pct is not None:
        with contextlib.suppress(ValueError, TypeError):
            return f"{country_key}_{suffix}", float(pct)
    return None, None


def init_entry():
    """Initialize an entry with all countries set to 0."""
    entry = {f"{c}_origin_pct": 0.0 for c in SOVIET_COUNTRIES}
    entry.update({f"{c}_birth_pct": 0.0 for c in SOVIET_COUNTRIES})
    return entry


def process_row(row):
    """Process a single census row and return (yishuv_sta, entry) or None."""
    yishuv_sta = parse_yishuv_sta(row[1], row[2])
    if not yishuv_sta:
        return None

    entry = init_entry()

    # Origin columns (4 countries): cols 61,62,63,64,65,66,67,68
    for i in range(4):
        key, val = extract_country_pct(row, 61 + i * 2, 62 + i * 2, "origin_pct")
        if key:
            entry[key] = val

    # Birth columns (3 countries): cols 69,70,71,72,73,74
    for i in range(3):
        key, val = extract_country_pct(row, 69 + i * 2, 70 + i * 2, "birth_pct")
        if key:
            entry[key] = val

    # Calculate aggregate soviet totals
    entry["soviet_origin_pct"] = sum(
        entry.get(f"{c}_origin_pct", 0) for c in SOVIET_COUNTRIES
    )
    entry["soviet_birth_pct"] = sum(
        entry.get(f"{c}_birth_pct", 0) for c in SOVIET_COUNTRIES
    )

    return yishuv_sta, entry


def load_census_data():
    """Load and parse census data into country_data dict."""
    df = pl.read_csv(CENSUS_PATH, has_header=False, infer_schema=False)
    country_data = {}
    for row in df.iter_rows():
        result = process_row(row)
        if result:
            yishuv_sta, entry = result
            country_data[yishuv_sta] = entry
    return country_data


def update_geojson(country_data):
    """Update GeoJSON with country data, return (geojson, match_count)."""
    with GEOJSON_PATH.open() as f:
        geojson = json.load(f)

    matched = 0
    for feature in geojson["features"]:
        props = feature["properties"]
        yishuv_sta = str(props.get("YISHUV_STA", ""))

        if yishuv_sta in country_data:
            props.update(country_data[yishuv_sta])
            matched += 1
        else:
            props.update(init_entry())
            props["soviet_origin_pct"] = 0.0
            props["soviet_birth_pct"] = 0.0

    with GEOJSON_PATH.open("w") as f:
        json.dump(geojson, f)

    return geojson, matched


def get_centroid(geometry):
    """Calculate centroid of a geometry."""
    coords = []
    if geometry["type"] == "Polygon":
        coords = geometry["coordinates"][0]
    elif geometry["type"] == "MultiPolygon":
        coords = geometry["coordinates"][0][0]
    if not coords:
        return [0, 0]
    x = sum(c[0] for c in coords) / len(coords)
    y = sum(c[1] for c in coords) / len(coords)
    return [round(x, 6), round(y, 6)]


def compute_top_areas(geojson):
    """Compute top 20 areas for each color mode."""
    top_areas = {}
    for mode in COLOR_MODES:
        areas = []
        for feature in geojson["features"]:
            props = feature["properties"]
            value = props.get(mode, 0)
            if value and value > 0:
                name = props.get("SHEM_YIS_1") or props.get("SHEM_YISHU") or "Unknown"
                center = get_centroid(feature["geometry"])
                areas.append({
                    "name": name,
                    "value": round(value, 2),
                    "center": center,
                })
        areas.sort(key=lambda x: x["value"], reverse=True)
        top_areas[mode] = areas[:20]
    return top_areas


def print_stats(country_data):
    """Print statistics for each country."""
    print("\nCountry statistics (max values found):")
    for country, info in SOVIET_COUNTRIES.items():
        origin_vals = [
            country_data[k].get(f"{country}_origin_pct", 0) for k in country_data
        ]
        birth_vals = [
            country_data[k].get(f"{country}_birth_pct", 0) for k in country_data
        ]
        origin_max = max(origin_vals) if origin_vals else 0
        birth_max = max(birth_vals) if birth_vals else 0
        origin_count = sum(1 for v in origin_vals if v > 0)
        birth_count = sum(1 for v in birth_vals if v > 0)
        if origin_max > 0 or birth_max > 0:
            print(
                f"  {info['flag']} {info['en']:15} "
                f"origin: max={origin_max:5.1f}% ({origin_count:4d} areas)  "
                f"birth: max={birth_max:5.1f}% ({birth_count:4d} areas)"
            )


def main():
    """Main entry point."""
    country_data = load_census_data()
    print(f"Processed {len(country_data)} census rows")

    geojson, matched = update_geojson(country_data)
    print(f"Matched {matched} features in GeoJSON")

    # Compute and save top areas for each color mode
    top_areas = compute_top_areas(geojson)
    with TOP_AREAS_PATH.open("w") as f:
        json.dump(top_areas, f)
    print(f"Saved top areas to {TOP_AREAS_PATH}")

    print_stats(country_data)


if __name__ == "__main__":
    main()
