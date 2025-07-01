import geopandas as gpd
import osmnx as ox
import os

def main():
    """
    Downloads and processes administrative boundaries for a country in chunks
    to conserve memory. It fetches states, gets municipalities for each,
    applies strict filtering, and appends them to a single GeoPackage file.
    """
    # --------------------------------------------------------------------------
    # 1. CONFIGURATION
    # --------------------------------------------------------------------------
    place_name = "Germany"
    chunk_admin_level = '4'  # Level for chunks (states)
    target_admin_level = '8' # Level for desired features (municipalities)

    tags = {
        'boundary': 'administrative',
        'admin_level': target_admin_level
    }

    output_path = f"{place_name.lower()}_municipalities_admin{target_admin_level}.gpkg"
    
    # --------------------------------------------------------------------------
    # 2. INITIAL SETUP
    # --------------------------------------------------------------------------
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Removed existing file: {output_path}")

    processed_osm_ids = set()
    total_municipalities_saved = 0

    # --------------------------------------------------------------------------
    # 3. FETCH BOUNDARIES FOR FILTERING
    # --------------------------------------------------------------------------
    print("Step 1: Downloading main country boundary for spatial filtering...")
    try:
        # Get the main polygon for the entire country
        country_boundary = ox.geocode_to_gdf(place_name)
        country_geom = country_boundary.geometry.iloc[0]
        print("✔ Main country boundary downloaded.")
    except Exception as e:
        print(f"❌ Could not download country boundary. Cannot proceed. Error: {e}")
        return

    print(f"Step 2: Downloading state polygons for {place_name} (admin_level={chunk_admin_level})...")
    try:
        states = ox.features_from_place(place_name, {'boundary': 'administrative', 'admin_level': chunk_admin_level})
        print(f"✔ Found {len(states)} states to process as chunks.")
    except Exception as e:
        print(f"❌ Could not download state polygons. Cannot proceed. Error: {e}")
        return

    # --------------------------------------------------------------------------
    # 4. PROCESS EACH CHUNK (STATE)
    # --------------------------------------------------------------------------
    print("\nStep 3: Processing municipalities for each state...")
    for i, state in enumerate(states.itertuples()):
        state_name = getattr(state, 'name', f'State {i+1}')
        print(f"\n--- Processing chunk {i+1}/{len(states)}: {state_name} ---")

        try:
            print(f"  Downloading municipalities for {state_name}...")
            municipalities_chunk = ox.features_from_polygon(state.geometry, tags)

            if municipalities_chunk.empty:
                print("  No features returned from OSM for this chunk.")
                continue

            # --- ⭐️ NEW: Strict Data Cleaning and Filtering ⭐️ ---
            
            # 1. Explicitly filter for the correct admin_level.
            # This is a crucial check as OSM can sometimes return related features with other levels.
            initial_count = len(municipalities_chunk)
            municipalities_chunk = municipalities_chunk[
                municipalities_chunk['admin_level'] == target_admin_level
            ]
            print(f"  Filtering by admin_level='{target_admin_level}': {initial_count} -> {len(municipalities_chunk)} features")
            
            # 2. Ensure all geometries are completely within the main country boundary.
            # This removes any neighboring country's data or offshore anomalies.
            initial_count = len(municipalities_chunk)
            municipalities_chunk = municipalities_chunk[
                municipalities_chunk.geometry.within(country_geom)
            ]
            print(f"  Filtering by country boundary: {initial_count} -> {len(municipalities_chunk)} features")
            
            # 3. Filter out OSM IDs that have already been saved from previous chunks.
            initial_count = len(municipalities_chunk)
            municipalities_chunk = municipalities_chunk[~municipalities_chunk.index.isin(processed_osm_ids)]
            if len(municipalities_chunk) < initial_count:
                print(f"  Removed {initial_count - len(municipalities_chunk)} duplicate OSM IDs from prior chunks.")

            if municipalities_chunk.empty:
                print("  No new, valid municipalities to save in this chunk.")
                continue

            # 4. Keep only valid Polygon/MultiPolygon geometries and clean up columns.
            municipalities_chunk = municipalities_chunk[municipalities_chunk.geometry.type.isin(['Polygon', 'MultiPolygon'])]
            columns_to_keep = ['name', 'geometry', 'admin_level']
            columns_in_df = [col for col in columns_to_keep if col in municipalities_chunk.columns]
            municipalities_chunk = municipalities_chunk[columns_in_df]
            
            if municipalities_chunk.empty:
                print("  No valid polygons left after final cleaning.")
                continue

            # --- Save the Processed Chunk ---
            print(f"  ✅ Found {len(municipalities_chunk)} new municipalities. Saving to file...")
            write_mode = 'a' if total_municipalities_saved > 0 else 'w'
            
            municipalities_chunk.to_file(
                output_path,
                driver="GPKG",
                mode=write_mode,
                layer=f"{place_name.lower()}_municipalities"
            )

            processed_osm_ids.update(municipalities_chunk.index)
            total_municipalities_saved += len(municipalities_chunk)
            print(f"  Successfully saved. Total so far: {total_municipalities_saved}")

        except Exception as e:
            print(f"  ❌ An error occurred while processing {state_name}: {e}")
            print("  Skipping this chunk.")

    # --------------------------------------------------------------------------
    # 5. FINALIZATION
    # --------------------------------------------------------------------------
    print("\n-----------------------------------------------------")
    print("🎉 Processing complete.")
    print(f"Total unique municipalities saved: {total_municipalities_saved}")
    print(f"Data saved to: {output_path}")
    print("-----------------------------------------------------")


if __name__ == "__main__":
    main()
