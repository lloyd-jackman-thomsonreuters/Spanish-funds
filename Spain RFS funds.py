# -*- coding: utf-8 -*-
"""
Created on Mon Jul 24 12:59:09 2017

@author: Lloyd Jackman
"""

import bs4
import requests
import pandas as pd
from fuzzywuzzy import process
from qdb import qdb5
import numpy as np
import threading
import multiprocessing as mp
from itertools import repeat


def pullpage(n, fund_details_list):
    res = requests.get("http://www.cnmv.es/Portal/Consultas/MostrarListados.aspx?id=11&page=%s" % str(n))
    if not res.status_code == requests.codes.ok:
        print("Unable to open http://www.cnmv.es/Portal/Consultas/MostrarListados.aspx?id=11&page=%s" % str(n))
    listings = bs4.BeautifulSoup(res.text, "lxml")
    for link in listings.select('a'):
        if link.get('href') is not None:
            if link.get('href').startswith("IIC"):
                fund_link = link.get('href')
                fund_page_html = requests.get(root+fund_link)
                fund_page = bs4.BeautifulSoup(fund_page_html.text, "lxml")
                for span in fund_page.select('span'):
                    if span.get("id") == "ctl00_ContentPrincipal_lblSubtitulo":
                        fund_name = span.getText().strip()
                for td in fund_page.select('td'):
                    if td.get("data-th") == "Nº Registro oficial":
                        fund_reg_no = td.getText().strip()
                    elif td.get("data-th") == "Fecha registro oficial":
                        fund_reg_date = td.getText().strip()
                    elif td.get("data-th") == "Tipo IIC":
                        fund_type = td.getText().strip()
                    elif td.get("data-th") == "País":
                        fund_country = td.getText().strip()
                    else:
                        pass
                fund_details = (root+fund_link, fund_name, fund_reg_no, fund_reg_date, fund_type, fund_country)
                fund_details_list.append(fund_details)


def namematch(fund, umbrella_names):
    matched_name, score = process.extractOne(fund, umbrella_names, score_cutoff=95)
    return (matched_name, score)


if __name__ == '__main__':
    root = "https://www.cnmv.es/Portal/Consultas/"
    fund_details_list = []
    res = requests.get("http://www.cnmv.es/Portal/Consultas/MostrarListados.aspx?id=11&page=0")
    test = bs4.BeautifulSoup(res.text, "lxml")
    for span in test.select('span'):
        if span.get("id") == "ctl00_ContentPrincipal_wucRelacionRegistros_MF_wucPaginadorRepeater_lblInfoPaginacion":
            text = span.getText()
            max_pages = int(text.split(" ")[-1]) - 1

    threads = []

    for n in range(0, max_pages):
        t = threading.Thread(target=pullpage, args=(n, fund_details_list))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    labels = ["Link", "Fund Name", "Fund Reg No", "Fund Reg Date", "Fund Type", "Fund Country"]
    cnmv_df = pd.DataFrame.from_records(fund_details_list, columns=labels)

    conn = qdb5()

    umbrellas = pd.read_sql("SELECT DISTINCT F.UMBRELLA_ID, N.NAME, R.ISO3_CODE \
    FROM QDB.FUND F \
    LEFT JOIN QDB.ASSET_NAME N \
    ON N.ASSET_ID = F.UMBRELLA_ID \
    LEFT JOIN QDB.ASSET_ATTRIBUTE A \
    ON A.ASSET_ID = F.UMBRELLA_ID \
    LEFT JOIN QDB.FUND_REGISTERED_COUNTRY R \
    ON R.FUND_ID = F.UMBRELLA_ID \
    AND F.UMBRELLA_ID IS NOT NULL \
    AND N.LANGUAGE_ID = 2 \
    AND N.END_DATE > SYSDATE \
    AND A.ATTRIBUTE_ID = 1350 \
    AND A.ATTRIBUTE_VALUE_ID = 398 \
    AND R.END_DATE > SYSDATE", conn)

    conn.close()

    matched_names = []
    scores = []

    umbrella_names = list(umbrellas["NAME"].unique())
    spain_names = list(cnmv_df["Fund Name"])

    pool = mp.Pool()
    result = pool.starmap(namematch, zip(spain_names, repeat(umbrella_names)))
    pool.close()
    pool.join()

    for (matched_name, score) in result:
        matched_names.append(matched_name)
        scores.append(score)

    cnmv_df["Matched Name"] = np.array(matched_names)
    cnmv_df["Score"] = np.array(scores)

    cnmv_df.to_excel("CNMV test.xlsx", index=False)
