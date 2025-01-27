import json
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, exc, text as sql_text
import datetime
import os
import pandas as pd
import sys
import re

BASE_URL = "https://www.resultados-futbol.com"

args = sys.argv[1:]
print(args)
year = [arg for arg in args if len(arg) == 4]
tournament = [arg for arg in args if len(arg)>4]
print({'years': year, 'tournament': tournament})
tournaments_input =pd.read_csv("tournaments_input_V2.csv")

def fetch_html(url):
    try:
        res = requests.get(url)
        soup = BeautifulSoup(res.content, "html.parser")
        return soup
    except requests.exceptions.RequestException as e:
        print(f"Error al procesar {url}: {e}")
        return None
    
def update_dataframe(df_:pd.DataFrame, col_, function_):
    df_[col_] = df_.apply(function_, axis=1)
    df_ = df_.explode(col_).reset_index()
    for col in df_.columns:
        if 'index' in col:
            df_.drop(col, axis=1, inplace=True)
    return df_

def add_urls_with_years(row):
    base_url = row['url']
    soup = fetch_html(base_url)
    div_temporadas = soup.find("div", id="desplega_temporadas")
    if div_temporadas:
        temporadas = [
            li.text.strip().split(" ")[1] for li in div_temporadas.find_all("li")
        ]
    else:
        temporadas = []
    urls_with_years = [base_url+year for year in temporadas]
    return urls_with_years

def add_groups(row):
    url = row['urls_with_years']
    print({'url_with_years': url})
    soup = fetch_html(url)
    if not soup:
        return None
    links_grupos = []
    for group in soup.find_all('a'):
        if 'Grupo' in group.text.strip() and url[-4:] in group.get('href') and group.get('href'):
            link = BASE_URL + group['href'] if BASE_URL not in group['href'] else group['href']
            links_grupos.append(link)
    if not links_grupos:
        links_grupos.append(url + '/grupo1')
    print("---------------------LINKS GRUPOS------------------------")
    print(sorted(list(set(links_grupos))))
    return sorted(list(set(links_grupos)))

def add_journey(row):
    url = row['urls_with_groups']
    soup = fetch_html(url)
    div_jornadas = soup.find("div", id="desplega_jornadas")
    if div_jornadas:
        links_jornadas = [BASE_URL+li.find('a')['href'] for li in div_jornadas.find_all('li') if li.find('a')]
    else:
        links_jornadas = []
    return links_jornadas if links_jornadas else None

def fetch_links():
    tournaments_input_2 = update_dataframe(tournaments_input, 'urls_with_years', add_urls_with_years)
    tournaments_input_filtered = None
    if len(year) > 0:
        regex_year = "|".join(year)
    if len(tournament) > 0:
        regex_tournament = "|".join([re.escape(t.replace(" ", "_")) for t in tournament])

    if len(year) > 0 and len(tournament) > 0:
        tournaments_input_filtered = tournaments_input_2[
            (tournaments_input_2.urls_with_years.str.contains(regex_year, na=False)) &
            (tournaments_input_2.urls_with_years.str.contains(regex_tournament, na=False))
        ]
    elif len(year) > 0:
        tournaments_input_filtered = tournaments_input_2[
            tournaments_input_2.urls_with_years.str.contains(regex_year, na=False)
        ]
    elif len(tournament) > 0:
        tournaments_input_filtered = tournaments_input_2[
            tournaments_input_2.urls_with_years.str.contains(regex_tournament, na=False)
        ]
    tournaments_input_filtered_2 = update_dataframe(tournaments_input_filtered, 'urls_with_groups', add_groups)
    tournaments_input_filtered_3 = update_dataframe(tournaments_input_filtered_2, 'urls_with_journeys', add_journey)
    tournaments_input_filtered_3['tournament'] = tournaments_input_filtered_3['tournament'].apply(lambda x: x.replace("_", " ").replace("/", ""))
    tournaments_input_filtered_3['year'] = tournaments_input_filtered_3['urls_with_years'].apply(lambda x: x[-4:])
    tournaments_input_filtered_3.drop('urls_with_years', axis=1, inplace=True)
    tournaments_input_filtered_3.drop('urls_with_groups', axis=1, inplace=True)
    tournaments_input_filtered_3 = tournaments_input_filtered_3.rename(columns={'url': 'base_url', 'urls_with_journeys': 'url'})
    print(tournaments_input_filtered_3.columns)
    return tournaments_input_filtered_3

def get_match_data(url, año, pais, torneo):
    print({'torneo': torneo, 'año': año,'match_url': url})
    soup = fetch_html(url)
    date_span = soup.find("span", class_="jor-date")
    status_span = soup.find("span", class_="jor-status")
    teams_ul = soup.find("ul", id="crumbs")
    teams_li = (
        [li.text.strip() for li in teams_ul.find("li", class_="act")]
        if teams_ul
        else []
    )
    home_team = teams_li[0].split(" - ")[0] if len(teams_li) > 0 else ""
    away_team = teams_li[0].split(" - ")[1] if len(teams_li) > 0 else ""
    div_scores = soup.find("div", id="marcador")
    div_scores = (
        [div_score for div_score in div_scores.find_all("div", class_="resultado")]
        if div_scores
        else []
    )
    scores = []
    for div_scor in div_scores:
        sp = div_scor.text.strip()
        scores.append(sp)
    scores = "-".join(scores)
    ul_tournament_name = soup.find("ul", id="crumbs")
    li_tournament_name = (
        [li for li in ul_tournament_name if ul_tournament_name and "\n" not in li]
        if ul_tournament_name
        else []
    )
    tournament_name = (
        [a for a in li_tournament_name[1].find("a")]
        if len(li_tournament_name) > 0
        else []
    )
    all_spans = soup.find_all("span")
    local_scorers_list = []
    away_scorers_list = []
    local_yellow_card_list = []
    list_away_yellow_cards = []
    local_red_card_list = []
    list_away_red_cards = []
    statistic_list = None
    statistic_dict = {}
    for sp in all_spans:
        if "left" in sp.get("class", []) and "minutos" not in sp.get("class", []):
            local_scorers = [
                a.text
                for a in sp.find_all("a")
                if "Gol de" in sp.find("small").text and a != ""
            ]
            if len(local_scorers) > 0:
                local_scorers_list.append(local_scorers[0])
            local_yellow_cards = [
                a.text
                for a in sp.find_all("a")
                if "T. Amarilla" in sp.find("small").text and a != ""
            ]
            if len(local_yellow_cards) > 0:
                local_yellow_card_list.append(local_yellow_cards[0])
            local_red_cards = [
                a.text
                for a in sp.find_all("a")
                if "Tarjeta Roja a" in sp.find("small").text and a != ""
            ]
            if len(local_red_cards) > 0:
                local_red_card_list.append(local_red_cards[0])
        if "right" in sp.get("class", []) and "minutos" not in sp.get("class", []):
            away_scorers = [
                a.text
                for a in sp.find_all("a")
                if "Gol de" in sp.find("small").text and a != ""
            ]
            if len(away_scorers) > 0:
                away_scorers_list.append(away_scorers[0])
            away_yellow_cards = [
                a.text
                for a in sp.find_all("a")
                if "T. Amarilla" in sp.find("small").text and a != ""
            ]
            if len(away_yellow_cards) > 0:
                list_away_yellow_cards.append(away_yellow_cards[0])
            away_red_cards = [
                a.text
                for a in sp.find_all("a")
                if "Tarjeta Roja a" in sp.find("small").text and a != ""
            ]
            if len(away_red_cards) > 0:
                list_away_red_cards.append(away_red_cards[0])
    matchs_statistics = []
    div_statistic_table = soup.select("div.contentitem table")
    statistic_table = (
        [table.find("tbody") for table in div_statistic_table]
        if div_statistic_table
        else []
    )
    tr_elements = [tr for table in statistic_table for tr in table.find_all("tr")]
    for tr in tr_elements:
        if tr.find_all("td")[1].find("h6").text.strip() == "Posesión del balón":
            statistic_dict["local_ball_position"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_ball_position"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Goles":
            statistic_dict["local_goals"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_goals"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Tiros a puerta":
            statistic_dict["local_kicks_to_goals"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_kicks_to_goals"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Tiros fuera":
            statistic_dict["local_outside_kicks"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_outside_kicks"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Total tiros":
            statistic_dict["local_total_kicks"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_total_kicks"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Paradas del portero":
            statistic_dict["local_shortcuts"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_shortcuts"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Saques de esquina":
            statistic_dict["local_corner_kicks"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_corner_kicks"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Fueras de juego":
            statistic_dict["local_offside"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_offside"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Tarjetas Rojas":
            statistic_dict["local_red_cards"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_red_cards"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Asistencias":
            statistic_dict["local_assists"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_assists"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Tiros al palo":
            statistic_dict["local_crossbar_kicks"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_crossbar_kicks"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Lesiones":
            statistic_dict["local_lesions"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_lesions"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Sustituciones":
            statistic_dict["local_substitutions"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_substitutions"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Faltas":
            statistic_dict["local_faults"] = tr.find_all("td")[0].text.strip()
            statistic_dict["away_faults"] = tr.find_all("td")[2].text.strip()
        elif tr.find_all("td")[1].find("h6").text.strip() == "Penalti cometido":
            statistic_dict["local_commited_penalties"] = tr.find_all("td")[
                0
            ].text.strip()
            statistic_dict["away_commited_penalties"] = tr.find_all("td")[
                2
            ].text.strip()
    match_data = {
        "tournament": torneo,
        "date": date_span.text.strip() if date_span else None,
        "year": año,
        "country": pais,
        "status": status_span.text.strip() if status_span else None,
        "home_team": home_team,
        "away_team": away_team,
        "url": url,
        "score": scores,
        "local_scorers": (
            ", ".join(local_scorers_list) if len(local_scorers_list) > 0 else ""
        ),
        "away_scorers": (
            ", ".join(away_scorers_list) if len(away_scorers_list) > 0 else ""
        ),
        "local_yellow_cards": (
            ", ".join(local_yellow_card_list) if len(local_yellow_card_list) > 0 else ""
        ),
        "away_yellow_cards": (
            ", ".join(list_away_yellow_cards) if len(list_away_yellow_cards) > 0 else ""
        ),
        "local_red_cards": (
            ", ".join(local_red_card_list) if len(local_red_card_list) > 0 else ""
        ),
        "away_red_cards": (
            ", ".join(list_away_red_cards) if len(list_away_red_cards) > 0 else ""
        ),
        "match_statistics": json.dumps(statistic_dict, ensure_ascii=False),
    }
    return match_data


def get_match_links(df):
    matchs_data = []
    data_torneos = pd.DataFrame()
    for index, row in df.iterrows():
        torneo = row['tournament']
        url = row["url"]
        año = row["year"]
        pais = row["country"]
        soup = fetch_html(url)
        matchs = soup.find(id="tabla1")
        match_link_td = (
            [td for td in matchs.find_all("td", class_="cmm")] if matchs else []
        )
        match_links = [
            BASE_URL + td.find("a")["href"] for td in match_link_td if td.find("a")
        ]
        for link in match_links:
            matchs_data.append(get_match_data(link, año, pais, torneo))
    data_torneos = pd.concat([data_torneos, pd.json_normalize(matchs_data)])
    return data_torneos

def transform_data(df):
    match_statistics = pd.DataFrame()
    for index, row in df.iterrows():
        match_statistics_json = json.loads(row['match_statistics'])
        if match_statistics_json:
            df_ = pd.json_normalize(match_statistics_json)
            match_statistics = pd.concat([match_statistics, df_], ignore_index=True)
    return match_statistics

def save_csv_by_tournament_and_year(data_torneos):
    print({'data_torneos.columns': data_torneos.columns.tolist()})
    grouped = data_torneos.groupby(["tournament", "year"])
    for (torneo, año), group in grouped:
        output_dir = "CSV"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        filename = f"{torneo}_{año}.csv".replace(" ", "_").replace("/", "_")
        filepath = os.path.join(output_dir, filename)
        group.to_csv(filepath, index=False, encoding="utf-8-sig")
        print(f"Archivo guardado: {filepath}")

if __name__ == "__main__":
    tournaments_input = fetch_links()
    data_torneos = get_match_links(tournaments_input)
    print("----------------------data_torneos.shape---------------------")
    print(data_torneos.shape)
    print("----------------------data_torneos.columns---------------------")
    print(data_torneos.columns)
    match_statistics = transform_data(data_torneos)
    print("----------------------match_statistics.columns---------------------")
    print(match_statistics.columns)
    print("----------------------match_statistics.shape---------------------")
    print(match_statistics.shape)
    data_torneos_final = pd.concat([data_torneos, match_statistics], axis=1)
    print("----------------------data_torneos_final.head()--------------------")
    print(data_torneos_final.head())
    print("----------------------data_torneos_final.columns--------------------")
    print(data_torneos_final.columns)
    for col in data_torneos_final.columns:
        if col == 'match_statistics':
            data_torneos_final.drop(col, axis=1, inplace=True)
    save_csv_by_tournament_and_year(data_torneos_final)