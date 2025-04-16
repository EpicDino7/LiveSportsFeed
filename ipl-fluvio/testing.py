from bs4 import BeautifulSoup
import requests

HEADERS = {'User-Agent': 'Mozilla/5.0'}


url = "https://www.cricbuzz.com/cricket-match/live-scores"
res = requests.get(url, headers=HEADERS)
soup = BeautifulSoup(res.text, "html.parser")

links = []
    
match_blocks = soup.find_all("div", class_="cb-mtch-lst cb-col cb-col-100 cb-tms-itm")

x = match_blocks[0].find('a',href=True)
# y = x.split("/")
# z = "live-cricket-scores" in y

# print(x.text.strip().removesuffix(","))

for match in match_blocks:
    a_tag = match.find("a", href=True)
    if a_tag and "live-cricket-scores" in a_tag["href"].split("/"):
        title = a_tag.text.strip().removesuffix(',')
        url = "https://www.cricbuzz.com" + a_tag["href"]
        links.append((title, url))

# print(links[2])

sent_payloads = set()

def scrape_match_data(match_url):
    res = requests.get(match_url, headers=HEADERS)
    soup = BeautifulSoup(res.text, 'html.parser')

    try:
        title = soup.find("h1", class_="cb-nav-hdr").text.strip().split(",")[0]

        score_div = soup.find("div", class_="cb-min-bat-rw")
        score = score_div.text.strip().split(')')[0]+")" if score_div else "Score not found"

        return {
            "match": title,
            "score": score
        }
    except Exception as e:
        print(f"Error scraping data: {e}")
        return None
    
u = scrape_match_data(links[2][1])
print(u)