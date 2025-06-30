import geopandas as gpd
import osmnx as ox
import os

def main():
    """
    Downloads and processes administrative boundaries for a country in chunks
    to conserve memory. It fetches states, then gets the municipalities
    for each state, processes them, and appends them to a single GeoPackage file.
    """
    # --------------------------------------------------------------------------
    # 1. CONFIGURATION
    # --------------------------------------------------------------------------
    # Define the country and the administrative levels.
    # admin_level=4 typically corresponds to states in Germany (Bundesländer).
    # admin_level=8 corresponds to municipalities (Gemeinden).
    place_name = "Germany"
    chunk_admin_level = '4'  # Level for chunks (states)
    target_admin_level = '8' # Level for desired features (municipalities)

    # Define the tags to fetch the municipalities.
    tags = {
        'boundary': 'administrative',
        'admin_level': target_admin_level
    }

    # Define the output file path. GeoPackage is preferred for chunking.
    output_path = f"{place_name.lower()}_municipalities_admin{target_admin_level}.gpkg"
    
    # --------------------------------------------------------------------------
    # 2. INITIAL SETUP
    # --------------------------------------------------------------------------
    # Remove the output file if it already exists to ensure a fresh start.
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"Removed existing file: {output_path}")

    # Set to keep track of OSM IDs that have already been processed and saved.
    # This prevents duplicates if a municipality crosses a state boundary.
    processed_osm_ids = set()
    total_municipalities_saved = 0

    # --------------------------------------------------------------------------
    # 3. FETCH STATE POLYGONS (CHUNKS)
    # --------------------------------------------------------------------------
    print(f"Step 1: Downloading state polygons for {place_name} (admin_level={chunk_admin_level})...")
    try:
        states = ox.features_from_place(place_name, {'boundary': 'administrative', 'admin_level': chunk_admin_level})
        print(f"Found {len(states)} states to process as chunks.")
    except Exception as e:
        print(f"Could not download state polygons. Error: {e}")
        return # Exit if we can't get the chunks

    # --------------------------------------------------------------------------
    # 4. PROCESS EACH CHUNK (STATE)
    # --------------------------------------------------------------------------
    for i, state in enumerate(states.itertuples()):
        state_name = getattr(state, 'name', f'State {i+1}')
        print(f"\n--- Processing chunk {i+1}/{len(states)}: {state_name} ---")

        try:
            # Download all admin_level=8 polygons within the current state's geometry.
            # This is the core of the chunking approach.
            print(f"  Downloading municipalities for {state_name}...")
            municipalities_chunk = ox.features_from_polygon(state.geometry, tags)

            # --- Data Cleaning for the Chunk ---

            # 1. Filter out OSM IDs that have already been saved from previous chunks.
            # This is more reliable than dropping duplicates by name.
            if not municipalities_chunk.empty:
                original_count = len(municipalities_chunk)
                municipalities_chunk = municipalities_chunk[~municipalities_chunk.index.isin(processed_osm_ids)]
                if len(municipalities_chunk) < original_count:
                    print(f"  Removed {original_count - len(municipalities_chunk)} duplicate OSM IDs found in previous chunks.")

            if municipalities_chunk.empty:
                print(f"  No new municipalities to process for {state_name}.")
                continue

            # 2. Keep only Polygon and MultiPolygon geometries.
            municipalities_chunk = municipalities_chunk[municipalities_chunk.geometry.type.isin(['Polygon', 'MultiPolygon'])]

            # 3. Clean up columns for export.
            columns_to_keep = ['name', 'geometry', 'admin_level']
            columns_in_df = [col for col in columns_to_keep if col in municipalities_chunk.columns]
            municipalities_chunk = municipalities_chunk[columns_in_df]
            
            if municipalities_chunk.empty:
                print(f"  No valid municipality polygons left after filtering for {state_name}.")
                continue

            # --- Save the Processed Chunk ---

            print(f"  Found {len(municipalities_chunk)} new municipalities. Saving to file...")
            
            # For the very first chunk, create the file (mode='w').
            # For all subsequent chunks, append to the file (mode='a').
            write_mode = 'a' if total_municipalities_saved > 0 else 'w'
            
            municipalities_chunk.to_file(
                output_path,
                driver="GPKG",
                mode=write_mode,
                layer=f"{place_name.lower()}_municipalities" # Layer name is required for append mode
            )

            # Update the set of processed IDs and the total count.
            processed_osm_ids.update(municipalities_chunk.index)
            total_municipalities_saved += len(municipalities_chunk)
            print(f"  Successfully saved. Total municipalities so far: {total_municipalities_saved}")

        except Exception as e:
            print(f"  An error occurred while processing {state_name}: {e}")
            print("  Skipping this chunk.")

    # --------------------------------------------------------------------------
    # 5. FINALIZATION
    # --------------------------------------------------------------------------
    print("\n-----------------------------------------------------")
    print("Processing complete.")
    print(f"Total unique municipalities saved: {total_municipalities_saved}")
    print(f"Data saved to: {output_path}")
    print("-----------------------------------------------------")


if __name__ == "__main__":
    main()