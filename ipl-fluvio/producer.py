import requests
from bs4 import BeautifulSoup
import json
import time
from fluvio import Fluvio

fluvio = Fluvio.connect()
producer = fluvio.topic_producer("ipl-live-feed")

HEADERS = {'User-Agent': 'Mozilla/5.0'}


def get_live_match_links():
    url = "https://www.cricbuzz.com/cricket-match/live-scores"
    res = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")
    
    links = []
    
    match_blocks = soup.find_all("div", class_="cb-mtch-lst cb-col cb-col-100 cb-tms-itm")
    
    for match in match_blocks:
        a_tag = match.find("a", href=True)
        if a_tag and "live-cricket-scores" in a_tag["href"].split("/"):
            title = a_tag.text.strip().removesuffix(",")
            url = "https://www.cricbuzz.com" + a_tag["href"]
            links.append((title, url))
    
    return links

def check_ipl(url):
    res = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")

    div = soup.find("div", class_="cb-nav-subhdr cb-font-12")
    if div and "Indian Premier League 2025" in div.text:
        return True
    return False
    
def get_upcoming_match_links():
    url = "https://www.cricbuzz.com/cricket-match/live-scores/upcoming-matches"
    res = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")

    links = []

    match_blocks = soup.find_all("div", class_="cb-mtch-lst cb-col cb-col-100 cb-tms-itm")

    for match in match_blocks:
        a_tag = match.find("a", href=True)
        if a_tag and "live-cricket-scores" in a_tag["href"].split("/"):
            title = a_tag.text.strip().removesuffix(",")
            url = "https://www.cricbuzz.com" + a_tag["href"]
            if check_ipl(url):
                links.append((title, url))

    return links

def scrape_match_data(match_url):
    res = requests.get(match_url, headers=HEADERS)
    soup = BeautifulSoup(res.text, 'html.parser')

    try:
        title = soup.find("h1", class_="cb-nav-hdr").text.strip().split(",")[0]

        
        score_section = soup.find("div", class_="cb-min-bat-rw")
        score = score_section.text.strip().split(')')[0]+')' if score_section else "Score not found"

        
        batsmen = []
        batsman_rows = soup.select("div.cb-col.cb-col-100.cb-scrd-itms")[:2]
        for row in batsman_rows:
            name_tag = row.find("a")
            runs_tag = row.find_all("div", class_="cb-col cb-col-8 text-right")[:2]
            if name_tag and runs_tag:
                batsmen.append({
                    "name": name_tag.text.strip(),
                    "runs": runs_tag[0].text.strip(),
                    "balls": runs_tag[1].text.strip()
                })

        
        bowler = {}
        bowler_row = soup.select_one("div.cb-col.cb-col-100.cb-ltst-wgt-hdr")
        if bowler_row:
            bowler_name_tag = bowler_row.find("a")
            bowler_stats = bowler_row.find_all("div", class_="cb-col cb-col-8 text-right")
            if bowler_name_tag and len(bowler_stats) >= 2:
                bowler = {
                    "name": bowler_name_tag.text.strip(),
                    "overs": bowler_stats[0].text.strip(),
                    "runs_given": bowler_stats[1].text.strip()
                }

        return {
            "match": title,
            "score": score,
            "batsmen": batsmen,
            "bowler": bowler
        }

    except Exception as e:
        print(f"Error scraping data: {e}")
        return None


sent_payloads = set()

while True:
    matches = get_upcoming_match_links()

    if not matches:
        print("No upcoming matches found.")
    else:
        for match_title, match_url in matches:
            match_data = scrape_match_data(match_url)
            if match_data:
                payload_key = json.dumps(match_data)
                if payload_key not in sent_payloads:
                    producer.send(
                        key=match_data["match"].encode("utf-8"),
                        value=json.dumps(match_data).encode("utf-8")
                    )
                    print("Sent:", match_data)
                    sent_payloads.add(payload_key)

    time.sleep(10)


