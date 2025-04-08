# Import required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import traceback # Import traceback for better error reporting

# Define the number of events to process
num_events_to_process = 30

# Print current working directory for debugging
try:
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Fallback for environments where __file__ is not defined (e.g., interactive)
    print("Aviso: __file__ não definido. Usando o diretório de trabalho atual para salvar CSVs.")
    script_dir = os.getcwd()
print(f"Diretório de trabalho atual (para salvar CSVs): {script_dir}")

# Send a GET request to the main events page with headers
url = 'http://ufcstats.com/statistics/events/completed?page=all'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
print(f"Fazendo requisição para: {url}")
response = requests.get(url, headers=headers)

# --- List Initializations ---
# Create lists to store event data
event_names_list = []
event_urls_list = []
event_dates_list = []

# Create lists to store fight data (one entry per fighter per fight)
fight_event_names = []
fight_wl = []
fight_fighter = []
fight_kd = []
fight_str = []
fight_td = []
fight_sub_stats = [] # SUB from stats (submission attempts)
fight_weights = []
fight_methods = []
fight_sub_details = [] # SUB from method details
fight_rounds = []
fight_times = []

event_links_found = [] # Store found links before filtering

# --- Fetch Event Links ---
if response.status_code == 200:
    print("Requisição da lista de eventos bem-sucedida!")
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find event links robustly
    event_table = soup.find('table', class_='b-statistics__table-events')
    if event_table:
        all_links = event_table.find_all('a', class_='b-link', href=True)
        for link in all_links:
             # Check if it's an event details link and avoid duplicates
             if 'event-details' in link['href'] and link.get('href') not in [l.get('href') for l in event_links_found]:
                 event_links_found.append(link)
    else:
        # Fallback if table structure is different
        print("Aviso: Tabela de eventos não encontrada. Tentando encontrar links 'b-link' genéricos.")
        all_links = soup.find_all('a', class_='b-link', href=lambda href: href and 'event-details' in href)
        for link in all_links:
             if link.get('href') not in [l.get('href') for l in event_links_found]:
                  event_links_found.append(link)

    print(f"Encontrados {len(event_links_found)} links de detalhes de eventos únicos.")
    # Populate event_urls_list up to the limit
    event_urls_list = [link.get('href') for link in event_links_found[:num_events_to_process]]
    print(f"Processando os primeiros {len(event_urls_list)} eventos.")

else:
     print(f"Falha ao buscar lista de eventos. Status code: {response.status_code}")

# --- Helper Function for Safe Text Extraction ---
def safe_get_text(tag_list, index, default='0'):
    """Safely extracts stripped text from a list of BeautifulSoup tags at a given index."""
    try:
        # Ensure index is within bounds and the item exists
        if tag_list and 0 <= index < len(tag_list):
             # Ensure the item itself is not None before calling methods
             tag = tag_list[index]
             if tag:
                  return tag.get_text(strip=True)
        # Return default if index out of bounds or tag_list is None/empty/item is None
        return default
    except AttributeError:
         # Handle cases where the item at index exists but doesn't have get_text
         print(f"  Aviso: safe_get_text encontrou um item inesperado (sem get_text) no índice {index}.")
         return default
    except Exception as e:
        # Catch any other unexpected errors during text extraction
        print(f"  Erro inesperado em safe_get_text: {e}")
        return default

# --- Process Each Event ---
for event_url in event_urls_list:
    print(f"\nProcessando evento: {event_url}")
    try:
        # Make request for the specific event page
        event_response = requests.get(event_url, headers=headers, timeout=20) # Added timeout
        event_response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        print("  Requisição do evento bem-sucedida!")
        event_soup = BeautifulSoup(event_response.content, 'html.parser')

        # --- Extract Event Details ---
        event_name_tag = event_soup.find('h2', class_='b-content__title')
        event_name = event_name_tag.get_text(strip=True) if event_name_tag else 'Name not found'
        print(f"  Nome do evento: {event_name}")

        # Find event date more reliably
        event_date = 'Date not found'
        event_info_list = event_soup.find('ul', class_='b-list__box-list')
        if event_info_list:
             list_items = event_info_list.find_all('li', class_='b-list__box-list-item')
             for item in list_items:
                  # Look for the 'Date:' text, case-insensitive and removing extra whitespace
                  item_text_cleaned = ' '.join(item.text.split())
                  if item_text_cleaned.lower().startswith('date:'):
                       event_date = item_text_cleaned.replace('Date:', '').strip()
                       break # Found the date, exit loop
        print(f"  Data do evento: {event_date}")

        # Append event data (only once per event)
        event_names_list.append(event_name)
        event_dates_list.append(event_date)

        # --- Find Fight Rows ---
        # Find the fight details table body for more specific targeting
        fight_table_body = event_soup.find('tbody', class_='b-fight-details__table-body')
        if not fight_table_body:
            print("  Aviso: Tabela de lutas (tbody) não encontrada para este evento.")
            continue # Skip to next event if no fight table body found

        # Find all fight rows within the table body
        fight_rows = fight_table_body.find_all('tr', class_='b-fight-details__table-row')
        print(f"  Encontradas {len(fight_rows)} lutas.")

        # --- Process Each Fight Row ---
        for i, fight in enumerate(fight_rows):
            print(f"\n  Processando Luta {i+1}...")
            try:
                # Extract all columns (td elements) from the current fight row
                columns = fight.find_all('td', class_='b-fight-details__table-col')
                # Check if we have the expected number of columns (at least 10 for indices 0-9)
                if len(columns) < 10:
                    print(f"  Aviso: Linha de luta {i+1} com colunas insuficientes ({len(columns)}), pulando.")
                    continue

                # --- Extract Fighter Names (Expected in Column 1) ---
                fighter_ps_col1 = columns[1].find_all('p', class_='b-fight-details__table-text')
                if len(fighter_ps_col1) < 2:
                     # If fighter names aren't found here, the structure differs significantly
                     print(f"  Aviso CRÍTICO: Não foi possível encontrar os dois nomes de lutadores na COLUNA 1 da luta {i+1}. Verifique a estrutura HTML desta coluna. Pulando luta.")
                     continue
                fighter1_name = safe_get_text(fighter_ps_col1, 0, 'Fighter 1 not found')
                fighter2_name = safe_get_text(fighter_ps_col1, 1, 'Fighter 2 not found')
                print(f"    Lutadores: {fighter1_name} vs {fighter2_name}")

                # --- Determine Outcome Status from Column 0 (Expected Location) ---
                # Find the <p> tags within Column 0 which should contain the status indicators
                status_ps_col0 = columns[0].find_all('p', class_='b-fight-details__table-text')

                # Find the specific status text tag INSIDE each <p> tag (if they exist)
                status_tag1 = None
                status_tag2 = None

                if len(status_ps_col0) > 0 and status_ps_col0[0]:
                    # Navigate within the first <p> of Col 0 to find the status text tag
                    status_tag1 = status_ps_col0[0].find('i', class_='b-flag__text')
                if len(status_ps_col0) > 1 and status_ps_col0[1]:
                    # Navigate within the second <p> of Col 0 to find the status text tag
                    status_tag2 = status_ps_col0[1].find('i', class_='b-flag__text')

                # Extract text from the found status tags, default to 'n/a' if not found
                fighter1_status_text = status_tag1.get_text(strip=True).lower() if status_tag1 else 'n/a'
                fighter2_status_text = status_tag2.get_text(strip=True).lower() if status_tag2 else 'n/a'

                # --- DEBUG --- Verify what was found/extracted ---
                print(f"    DEBUG Status <p> Tags Found in Col 0: {len(status_ps_col0)}")
                print(f"    DEBUG Status Text Tags Found (in Col 0 <p>s): Tag1={'Found' if status_tag1 else 'Not Found'}, Tag2={'Found' if status_tag2 else 'Not Found'}")
                print(f"    DEBUG Status Texts Extracted from Tags: F1='{fighter1_status_text}', F2='{fighter2_status_text}'")
                # --- END DEBUG ---

                # --- Determine Final Status ('Win' or 'Loss') ---
                status1_final = 'Loss' # Default to Loss
                status2_final = 'Loss' # Default to Loss

                if fighter1_status_text == 'win':
                    status1_final = 'Win'
                    status2_final = 'Loss'
                elif fighter2_status_text == 'win':
                    status1_final = 'Loss'
                    status2_final = 'Win'
                elif fighter1_status_text == 'draw' or fighter2_status_text == 'draw':
                    status1_final = 'Loss' # Treat Draw as Loss per user request
                    status2_final = 'Loss'
                    print(f"    Info: Luta {i+1} detectada como Empate (Draw). Marcando ambos como 'Loss'.")
                else:
                    # If not 'win' or 'draw', check for NC based on METHOD column
                    # (Need to extract method first for this check)
                    method_col_ps = columns[7].find_all('p', class_='b-fight-details__table-text')
                    method = safe_get_text(method_col_ps, 0, 'N/A')
                    if 'no contest' in method.lower() or 'nc' == method.lower():
                         status1_final = 'Loss' # Treat NC as Loss per user request
                         status2_final = 'Loss'
                         print(f"    Info: Luta {i+1} detectada como No Contest pelo método. Marcando ambos como 'Loss'.")
                    else:
                        # If not Win/Draw/NC, keep the default 'Loss'.
                        # Print a warning only if text was extracted but not recognized.
                        if fighter1_status_text != 'n/a' or fighter2_status_text != 'n/a':
                             print(f"    Aviso: Resultado 'Win'/'Draw' não reconhecido nos status texts de Col 0 para luta {i+1}. Status lidos: F1='{fighter1_status_text}', F2='{fighter2_status_text}'. Marcando como 'Loss'.")

                print(f"    Resultado Final -> {fighter1_name}: {status1_final}, {fighter2_name}: {status2_final}")

                # --- Extract Paired Stats (Expected in Columns 2-5) ---
                # Helper function defined within the loop or outside (if preferred)
                def get_stats_pair(col_index):
                    # Safely access the column first
                    if col_index < len(columns):
                        stat_tags = columns[col_index].find_all('p', class_='b-fight-details__table-text')
                        stat1 = safe_get_text(stat_tags, 0, '0')
                        stat2 = safe_get_text(stat_tags, 1, '0')
                        return stat1, stat2
                    else:
                        print(f"    Aviso: Tentativa de acessar coluna de estatística inexistente (índice {col_index}) na luta {i+1}.")
                        return '0', '0' # Return default if column index is invalid

                kd1, kd2 = get_stats_pair(2)      # KD expected in Col index 2
                str1, str2 = get_stats_pair(3)    # STR expected in Col index 3
                td1, td2 = get_stats_pair(4)      # TD expected in Col index 4
                sub1, sub2 = get_stats_pair(5)    # SUB stats expected in Col index 5

                # --- Extract Common Fight Details (Expected in Columns 6-9) ---
                # Weight Class (Col 6)
                weight_col_p = columns[6].find('p', class_='b-fight-details__table-text') if len(columns) > 6 else None
                weight = weight_col_p.get_text(strip=True).split('\n')[0].strip() if weight_col_p else 'N/A'

                # Method & Detail (Col 7)
                method_col_ps = columns[7].find_all('p', class_='b-fight-details__table-text') if len(columns) > 7 else []
                method = safe_get_text(method_col_ps, 0, 'N/A') # Re-extract method if needed
                sub_detail = safe_get_text(method_col_ps, 1, '') # Method detail

                # Round (Col 8)
                round_col_p = columns[8].find('p', class_='b-fight-details__table-text') if len(columns) > 8 else None
                round_num = round_col_p.get_text(strip=True) if round_col_p else 'N/A'

                # Time (Col 9)
                time_col_p = columns[9].find('p', class_='b-fight-details__table-text') if len(columns) > 9 else None
                time = time_col_p.get_text(strip=True) if time_col_p else 'N/A'


                # --- Append Row for Fighter 1 ---
                fight_event_names.append(event_name)
                fight_wl.append(status1_final)
                fight_fighter.append(fighter1_name)
                fight_kd.append(kd1)
                fight_str.append(str1)
                fight_td.append(td1)
                fight_sub_stats.append(sub1)
                fight_weights.append(weight)
                fight_methods.append(method)
                fight_sub_details.append(sub_detail)
                fight_rounds.append(round_num)
                fight_times.append(time)

                # --- Append Row for Fighter 2 ---
                fight_event_names.append(event_name)
                fight_wl.append(status2_final)
                fight_fighter.append(fighter2_name)
                fight_kd.append(kd2)
                fight_str.append(str2)
                fight_td.append(td2)
                fight_sub_stats.append(sub2)
                fight_weights.append(weight)
                fight_methods.append(method)
                fight_sub_details.append(sub_detail)
                fight_rounds.append(round_num)
                fight_times.append(time)

            # --- Error Handling for Individual Fight Processing ---
            except Exception as e:
                print(f"  Erro CRÍTICO ao processar dados da luta {i+1}: {e}")
                print(f"  Lutadores: {fighter1_name if 'fighter1_name' in locals() else 'N/D'} vs {fighter2_name if 'fighter2_name' in locals() else 'N/D'}")
                print("--- Traceback ---")
                traceback.print_exc()
                print("--- Fim Traceback ---")
                # Continue to the next fight instead of stopping the script
                continue

    # --- Error Handling for Event Page Request/Processing ---
    except requests.exceptions.RequestException as e:
         print(f"  Falha na requisição do evento {event_url}. Erro: {e}")
    except Exception as e:
         print(f"  Erro inesperado ao processar o evento {event_url}: {e}")
         print("--- Traceback ---")
         traceback.print_exc()
         print("--- Fim Traceback ---")


# --- Create DataFrames ---
print(f"\nCriando DataFrames...")
print(f"Número de eventos na lista: {len(event_names_list)}")
print(f"Número de linhas de lutador na lista: {len(fight_event_names)}") # Should be approx 2 * num_fights

# Event DataFrame
event_df = pd.DataFrame({
    'Event Name': event_names_list,
    'Event URL': event_urls_list, # Should match length of event_names_list
    'Event Date': event_dates_list
})

# Fight DataFrame
# Ensure all lists are the same length before creating DataFrame
# This is a basic check; more robust checks might involve comparing lengths
if not all(len(lst) == len(fight_event_names) for lst in [fight_wl, fight_fighter, fight_kd, fight_str, fight_td, fight_sub_stats, fight_weights, fight_methods, fight_sub_details, fight_rounds, fight_times]):
    print("\n!!! ALERTA: Inconsistência no comprimento das listas de dados das lutas. O DataFrame pode estar incorreto ou vazio. !!!")
    # Optionally print lengths for debugging:
    print(f"Lengths: Event={len(fight_event_names)}, WL={len(fight_wl)}, Fighter={len(fight_fighter)}, KD={len(fight_kd)}, STR={len(fight_str)}, TD={len(fight_td)}, SUBst={len(fight_sub_stats)}, Weight={len(fight_weights)}, Method={len(fight_methods)}, SUBdet={len(fight_sub_details)}, Rnd={len(fight_rounds)}, Time={len(fight_times)}")
    # Decide how to handle: exit, create partial DataFrame, fill missing?
    # For now, let's allow creation, but the warning is important.

fight_df = pd.DataFrame({
    'Event Name': fight_event_names,
    'W/L': fight_wl,
    'FIGHTER': fight_fighter,
    'KD': fight_kd,
    'STR': fight_str,
    'TD': fight_td,
    'SUB (stats)': fight_sub_stats, # Submission Attempts
    'WEIGHT CLASS': fight_weights,
    'METHOD': fight_methods,
    'SUB': fight_sub_details,       # Method detail (type of submission etc)
    'ROUND': fight_rounds,
    'TIME': fight_times
})

# --- Print DataFrames ---
print("\nEvents:")
# Display settings for better readability
with pd.option_context('display.max_rows', 10, 'display.max_colwidth', 40):
     print(event_df)

print("\nFights (2 rows per fight):")
if not fight_df.empty:
    # Display settings for better readability
    with pd.option_context('display.max_rows', 20, 'display.max_columns', None, 'display.width', 120):
         print(fight_df)
else:
    print("DataFrame de Lutas está vazio.")

# --- Save to CSV files ---
event_csv_path = os.path.join(script_dir, 'event_data.csv')
fight_csv_path = os.path.join(script_dir, 'fight_data_by_fighter.csv') # Keep the specific name
try:
    event_df.to_csv(event_csv_path, index=False, encoding='utf-8-sig')
    fight_df.to_csv(fight_csv_path, index=False, encoding='utf-8-sig')
    print(f"\nDados salvos com sucesso em:")
    print(f"  {event_csv_path}")
    print(f"  {fight_csv_path}")