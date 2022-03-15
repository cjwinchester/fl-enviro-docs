import os
import csv
import time

import requests
from slugify import UniqueSlugify

from scrape import CSV_FILE


def dl_docs():

    slug = UniqueSlugify(to_lower=True)

    with open(CSV_FILE, 'r') as infile:
        data = list(csv.DictReader(infile))

    for row in data:
        f_id = row['facility_id']
        dir_path = f'./{f_id}'

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        doc_slug = f'{f_id}-{row["document_date"]}-{row["document_title"]}-{row["document_category"]}'  # noqa
        filename = slug(doc_slug)
        file_path = f'{dir_path}/{filename}'

        url = row['document_link']

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            if 'pdf' in r.headers.get('content-type'):
                file_path = f'{file_path}.pdf'
            print(f'Downloading {file_path} ...')
            with open(file_path, 'wb') as outfile:
                for chunk in r.iter_content(chunk_size=8192):
                    outfile.write(chunk)
            print('    Done!')

        time.sleep(0.5)


if __name__ == '__main__':
    dl_docs()
