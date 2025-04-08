# Importar biblioteca
import requests  # Fazer requisições HTTP
from bs4 import BeautifulSoup  # Parsear HTML
import pandas as pd  # Manipulação de dados em tabelas

num_events_to_process = None  # Define o número de eventos a serem processados - None traz tudo

url = 'http://ufcstats.com/statistics/events/completed?page=all'  # URL da página com todos os eventos do UFC
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124'}  # Simula um navegador para evitar bloqueios
response = requests.get(url, headers=headers)  # Faz a requisição GET para a página principal
soup = BeautifulSoup(response.content, 'html.parser')  # Converte o conteúdo da resposta em um objeto BeautifulSoup para parsing

# Extrai os links dos eventos, limitando ao número definido
event_urls = [link['href'] for link in soup.find_all('a', class_='b-link', href=lambda href: href and 'event-details' in href)][:num_events_to_process]

# Inicializa listas para armazenar nome, data e URL dos eventos
event_names, event_dates, event_urls_list = [], [], []  

# Inicializa dicionário para armazenar dados das lutas
fight_data = {'Event Name': [], 'W/L': [], 'FIGHTER': [], 'KD': [], 'STR': [], 'TD': [], 'SUB (stats)': [], 
              'WEIGHT CLASS': [], 'METHOD': [], 'SUB': [], 'ROUND': [], 'TIME': []}

# Itera sobre cada URL de evento para extrair dados
for event_url in event_urls:
    event_soup = BeautifulSoup(requests.get(event_url, headers=headers).content, 'html.parser')  # Faz requisição e parseia a página do evento
    event_name = event_soup.find('h2', class_='b-content__title').get_text(strip=True)  # Extrai o nome do evento do título
    # Extrai a data do evento da lista de informações, com fallback se não encontrada
    event_date = next((item.text.split('Date:')[1].strip() for item in event_soup.find('ul', class_='b-list__box-list').find_all('li') if 'Date:' in item.text), 'Date not found')
    
    event_names.append(event_name)  # Adiciona o nome do evento à lista
    event_dates.append(event_date)  # Adiciona a data do evento à lista
    event_urls_list.append(event_url)  # Adiciona a URL do evento à lista
    
    # Itera sobre cada linha de luta na tabela do evento
    for fight in event_soup.find('tbody', class_='b-fight-details__table-body').find_all('tr', class_='b-fight-details__table-row'):
        cols = fight.find_all('td', class_='b-fight-details__table-col')  # Obtém todas as colunas da linha da luta
        fighters = [p.get_text(strip=True) for p in cols[1].find_all('p')]  # Extrai os nomes dos lutadores da coluna 1
        
        status_tags = cols[0].find_all('p')  # Obtém os elementos de status (vitória/derrota) da coluna 0
        # Extrai o texto do status, usando 'loss' como padrão se não houver indicador
        status = [tag.find('i', class_='b-flag__text').get_text(strip=True).lower() if tag.find('i') else 'loss' for tag in status_tags]
        # Garante que a lista de vitórias/derrotas tenha 2 elementos, convertendo para 'Win' ou 'Loss'
        wl = ['Win' if s == 'win' else 'Loss' for s in (status + ['loss', 'loss'])[:2]]
        
        # Define função para extrair estatísticas, garantindo 2 elementos com '0' como padrão
        stats = lambda i: [p.get_text(strip=True) for p in cols[i].find_all('p')] + ['0'] * (2 - len(cols[i].find_all('p')))
        kd, str_, td, sub = stats(2), stats(3), stats(4), stats(5)  # Extrai knockdowns, golpes, quedas e submissões
        
        # Extrai a classe de peso da coluna 6
        # Encontra a tag <p> primeiro para evitar erro se não existir
        weight_p_tag = cols[6].find('p')
        # Extrai o texto, pega apenas a primeira linha (caso haja quebras), e remove espaços
        weight = weight_p_tag.get_text(strip=True).split('\n')[0].strip() if weight_p_tag else 'N/A' # Define 'N/A' se a tag <p> não for encontrada

        method_details = [p.get_text(strip=True) for p in cols[7].find_all('p')]  # Extrai método e detalhes da finalização da coluna 7
        round_ = cols[8].find('p').get_text(strip=True)  # Extrai o round da coluna 8
        time = cols[9].find('p').get_text(strip=True)  # Extrai o tempo da luta da coluna 9
        
        # Adiciona dados de cada lutador (2 por luta) ao dicionário
        for i in range(2):
            fight_data['Event Name'].append(event_name)  # Nome do evento
            fight_data['W/L'].append(wl[i])  # Resultado (Win/Loss)
            fight_data['FIGHTER'].append(fighters[i])  # Nome do lutador
            fight_data['KD'].append(kd[i])  # Knockdowns
            fight_data['STR'].append(str_[i])  # Golpes significativos
            fight_data['TD'].append(td[i])  # Quedas
            fight_data['SUB (stats)'].append(sub[i])  # Tentativas de submissão
            fight_data['WEIGHT CLASS'].append(weight)  # Classe de peso
            fight_data['METHOD'].append(method_details[0])  # Método de finalização
            fight_data['SUB'].append(method_details[1] if len(method_details) > 1 else '')  # Detalhes da submissão (se houver)
            fight_data['ROUND'].append(round_)  # Round
            fight_data['TIME'].append(time)  # Tempo da luta

# Cria os DataFrames com os dados dos eventos e das lutas
event_df = pd.DataFrame({'Event Name': event_names, 'Event URL': event_urls_list, 'Event Date': event_dates})
fight_df = pd.DataFrame(fight_data)

# Salva os Dataframes em um arquivo CSV
event_df.to_csv('event_data.csv', index=False)  
fight_df.to_csv('fight_data.csv', index=False) 