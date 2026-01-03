# Soviet Jews in Israel - Mapped

## Overview

This project displays the percentage of residents in each Israeli statistical area whose origin or birthplace is a former Soviet country. Users can explore the data by country (Russia, Ukraine, Belarus, etc.) and by type (origin vs. birthplace), with the top results highlighted for each metric.

## Data Sources

- **Census Data**: Israeli Central Bureau of Statistics (CBS) 2022 Census at the statistical area level
- **Geographic Boundaries**: CBS Statistical Areas 2022 boundaries

## Project Structure

```
soviet_jews/
├── index.html              # Main web application
├── main.py                 # Data processing script
├── server.py               # HTTP server with Range request support
├── pyproject.toml          # Python project configuration
├── data/
│   └── census_stat_areas.xlsx   # CBS census data
└── statistical_areas_2022/
    ├── statistical_areas.geojson  # Processed GeoJSON with census data
    ├── statistical_areas.pmtiles  # Vector tiles for map display
    └── statistical_areas_2022.fgb # Original FlatGeobuf boundaries
```

## Technical Details

### Frontend

- MapLibre GL JS (v4.7.1) - Vector map rendering
- PMTiles (v3.2.0) - Efficient tile serving without a tile server
- OpenFreeMap - Base map tiles (Liberty style)

### Backend

- pandas - Census data processing
- openpyxl - Excel file reading
- Custom HTTP server with Range request support for PMTiles

### Data Pipeline

1. Census data is read from the CBS Excel file
2. Hebrew country names are matched to standardized country keys
3. Percentages are extracted for top-4 origin countries and top-3 birth countries
4. Data is joined to GeoJSON by `YISHUV_STA` (settlement + statistical area code)
5. GeoJSON is converted to PMTiles for efficient map rendering

## TODO

- [ ] Age cohort analysis: Add age distribution data to identify potential survivors of Stalinist repression (born before ~1940) vs. their descendants, enabling demographic filtering
- [ ] Aliyah wave segmentation: Integrate immigration timing data to distinguish between:
  - 1970-1988 wave (pre-collapse emigration)
  - 1990s wave (post-Soviet mass aliyah)
  - Recent immigration (2000s-present)
- [ ] Absolute population counts: Add raw population numbers alongside percentages to identify areas with largest Soviet-origin communities (not just highest concentration)
