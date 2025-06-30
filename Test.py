from shapely.geometry import Point
import pandas as pd
import geopandas as gpd
import osmnx as ox

# Definieer het administratieve niveau en de plaats
place_name = "Germany"
admin_level = "8"

# Definieer tags om te filteren op admin_level 8 grenzen (gemeenten)
tags = {
    'boundary': 'administrative',
    'admin_level': admin_level
}

# Haal GeoDataFrame op met administratieve grenzen
print("Bezig met downloaden van gemeente-polygonen voor", place_name, "(admin_level=8)...")
municipalities = ox.features_from_place(place_name, tags)

# Verwijder dubbele entries op basis van naam
# Kan sws beter
# duplicates op basis van osm-id vindin? (future)
municipalities = municipalities.drop_duplicates(subset='name')

# Behoud enkel Polygon- en MultiPolygon-geometrieen
municipalities = municipalities[municipalities.geometry.type.isin(['Polygon', 'MultiPolygon'])] # Voeg 'MultiPolygon' later toe

# Haal de grenspolygon van België op
country_boundary = ox.geocode_to_gdf(place_name)

# Filter alleen de entries die admin_level = 8 hebben
municipalities = municipalities[municipalities['admin_level'] == '8']


# Behoud enkel gemeenten die volledig binnen de officiële Belgische grens vallen
municipalities = municipalities[municipalities.geometry.within(country_boundary.loc[0, 'geometry'])]

# Kolommen opschonen voor export of inspectie
columns_to_keep = ['name', 'geometry', 'admin_level']
columns_to_keep = [col for col in columns_to_keep if col in municipalities.columns]
municipalities = municipalities[columns_to_keep]

# Toon wat info enz
print(f"{len(municipalities)} gemeente-polygonen overgehouden na filtering.")
print(municipalities.head())

# Opslaan als GeoJSON
# Misschien GEOPackage? (future)
output_path = f"{place_name.lower()}_municipalities_admin8.geojson"
municipalities.to_file(output_path, driver="GeoJSON")
print(f"GeoJSON opgeslagen als: {output_path}")
