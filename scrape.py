import csv
from urllib.parse import urlparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup

CSV_FILE = 'permitting_records.csv'


def gather_links():

    csv_headers = [
        'source_url',
        'facility_id',
        'facility_name',
        'document_category',
        'document_date',
        'document_title',
        'document_link'
    ]

    urls_t1_pattern = 'https://fldeploc.dep.state.fl.us/WWW_WACS/Reports/SW_Facility_Docs.asp?wacsid={}'  # noqa

    urls_t2_pattern = 'https://prodenv.dep.state.fl.us/DepNexus/public/electronic-documents/{}/gis-facility!search'  # noqa

    class t1Facility(object):
        def __init__(self, facility_id):
            self.id = facility_id
            self.url = urls_t1_pattern.format(self.id)
            self.html = requests.get(self.url)
            self.soup = BeautifulSoup(
                self.html.text,
                'html.parser'
            )

        def scrape(self):
            data = []
            facil = self.soup.find_all('h3')[-1].a.text.strip()
            tables = self.soup.find_all('table')[1:]
            for table in tables:
                category = table.previous_sibling.previous_sibling.previous_sibling.previous_sibling.text.strip().lstrip('AND ').split(' DOCUMENTS LISTED HERE')[0]  # noqa
                rows = table.find_all('tr')
                for row in rows[1:]:
                    cells = row.find_all('td')
                    date, title, link = cells
                    date_clean = datetime.strptime(date.text.strip(), '%Y.%M.%d').date().isoformat()  # noqa
                    title_clean = ' '.join(title.text.split())
                    link_clean = link.a['href']

                    data_out = [
                        self.url,
                        self.id,
                        facil,
                        category,
                        date_clean,
                        title_clean,
                        link_clean
                    ]

                    data.append(dict(zip(csv_headers, data_out)))

            print(f'Grabbed {len(data)} document records from facility ID #{self.id}')  # noqa
            return data

    class t2Facility(object):
        def __init__(self, facility_id):
            self.id = facility_id
            self.url = urls_t2_pattern.format(self.id)
            self.url_parsed = urlparse(self.url)
            self.url_base = f'{self.url_parsed.scheme}://{self.url_parsed.netloc}'  # noqa
            self.html = requests.get(self.url)
            self.soup = BeautifulSoup(
                self.html.text,
                'html.parser'
            )

        def scrape(self):
            data = []

            csv_link = self.soup.find('a', {'class': 'jq-button'})['href']

            csv_link_full = f'{self.url_base}{csv_link}'

            text = requests.get(csv_link_full).text
            lines = text.splitlines()
            reader = csv.DictReader(lines)

            for row in reader:

                doc_date = datetime.strptime(
                    row['DOCUMENT DATE'],
                    '%M-%d-%Y').date().isoformat()  # noqa

                d = {
                    'source_url': self.url,
                    'facility_id': self.id,
                    'facility_name': row['FACILITY/SITE NAME'],
                    'document_category': row['DOCUMENT TYPE'],
                    'document_date': doc_date,
                    'document_title': row['SUBJECT'],
                    'document_link': row['FILE PATH'] 
                }

                data.append(d)

            print(f'Grabbed {len(data)} document records from facility ID #{self.id}')  # noqa
            return data

    facil_ids = {
        't1': [3133, 93916, 3051],
        't2': [85391, 95411]
    }

    all_data = []

    for key in facil_ids:
        for f_id in facil_ids[key]:
            if key == 't1':
                parsed = t1Facility(f_id)
            else:
                parsed = t2Facility(f_id)

            data = parsed.scrape()  
            all_data.extend(data)

    with open(CSV_FILE, 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(all_data)

    print('')


if __name__ == '__main__':
    gather_links()
