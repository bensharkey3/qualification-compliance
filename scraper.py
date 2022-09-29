import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime


URL1 = "https://www.rtiocompliance.scodle.com/verify/5BGQ000"
URL2 = "https://www.rtiocompliance.scodle.com/verify/649G000"

URLS = [URL1, URL2]

# create empty df, populate by looping through url's
df_all = pd.DataFrame(columns=['name', 'emailsap', 'qualname', 'obtained', 'expires'])

for i in URLS:
    page = requests.get(i)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find("body")

    elements_person = results.find_all("div", class_="userdetails")
    elements_current = results.find_all("div", class_="result current")
    elements_expired = results.find_all("div", class_="result expired")

    # create empty df, populate by looping through elements
    df_person = pd.DataFrame(columns=['name', 'emailsap'])

    for job_element in elements_person:
        lst = []
        name_element = job_element.find("h3")
        emailsap_element = job_element.find("h4")

        name = name_element.text.strip()
        emailsap = emailsap_element.text.strip()

        lst.append(name)
        lst.append(emailsap)
        lst = lst[0:2]
        df_person.loc[len(df_person)] = lst

    # create empty df, populate by looping through elements
    df_current = pd.DataFrame(columns=['qualname', 'obtained', 'expires'])

    for job_element in elements_current:
        lst = []
        qualname_element = job_element.find("h2")
        obtained_element = job_element.find("span", class_="obtained")
        expires_element = job_element.find("span", class_="expires")

        qualname = qualname_element.text.strip()
        obtained = obtained_element.text.strip()
        expires = expires_element.text.strip()

        lst.append(qualname)
        lst.append(obtained)
        lst.append(expires)
        lst = lst[0:3]
        df_current.loc[len(df_current)] = lst

    # create empty df, populate by looping through elements
    df_expired = pd.DataFrame(columns=['qualname', 'obtained', 'expires'])

    for job_element in elements_expired:
        lst = []
        qualname_element = job_element.find("h2")
        expires_element = job_element.find("span", class_="expired")

        qualname = qualname_element.text.strip()
        obtained = pd.NaT
        expires = expires_element.text.strip()

        lst.append(qualname)
        lst.append(obtained)
        lst.append(expires)
        lst = lst[0:3]
        df_expired.loc[len(df_expired)] = lst


    # join and concat the url's 3x df's into one
    df_person = df_person[0:1]
    df_allquals = pd.concat([df_current, df_expired])
    df_allquals.reset_index(inplace=True, drop=True)

    df_allquals['joincol'] = 0
    df_combined = df_person.merge(df_allquals, how='left', left_index=True, right_on='joincol')
    df_combined = df_combined.drop('joincol', axis=1)
    
    df_all = pd.concat([df_all, df_combined], axis=0)


# convert datetime columns
df = df_all.copy()

df['todays_date'] = pd.to_datetime('now')
df['obtained'] = pd.to_datetime(df['obtained'])
df['expires'] = pd.to_datetime(df['expires'])

# create calculated columns
df['days_to_expiry'] = (df['expires'] - df['todays_date']).dt.days

df['expiry_category'] = np.where(df['expires'] <= df['todays_date'], 'expired', 
                            np.where((df['expires'] <= df['todays_date'] + pd.Timedelta(days=30)) & 
                                    (df['expires'] > df['todays_date']), 'expires within 30 days',
                            np.where((df['expires'] <= df['todays_date'] + pd.Timedelta(days=60)) & 
                                    (df['expires'] > df['todays_date']), 'expires within 60 days',
                            np.where((df['expires'] <= df['todays_date'] + pd.Timedelta(days=90)) & 
                                    (df['expires'] > df['todays_date']), 'expires within 90 days','valid'))))

# formatting
df['todays_date'] = df['todays_date'].dt.date

df['name'] = df['name'].str.replace(r'Name: ', '')
df['emailsap'] = df['emailsap'].str.replace(r'SAP#: ', '')
df['emailsap'] = df['emailsap'].str.replace(r'Email: ', '')
