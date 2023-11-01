from flask import (Flask, render_template, request, send_file, send_from_directory)
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import os
import requests
from datetime import datetime
import time
from markupsafe import Markup

API_KEY = "052da271-b6d6-4a4a-aa85-ef81b44c5c80"
BASE_URL = "https://api.company-information.service.gov.uk"

app = Flask(__name__)
loop = asyncio.get_event_loop()
company_index = 0
is_end = 0
search_started = 0
total_result = []
cur_page = 0
PAGE_COUNT = 10

def convert_date(date_str):
  date_obj = datetime.strptime(date_str, '%Y-%m-%d')
  return date_obj.strftime("%d %B %Y")

def check_year(date_str):
  date_obj = datetime.strptime(date_str, '%Y-%m-%d')
  if date_obj.year == 2023 or date_obj.year == 2024:
    return True
  return False

def process_429_response(response):
  print("Processing 429 Response")
  print(response.headers)
  limit = int(response.headers["X-RateLimit-Limit"])
  remain = int(response.headers["X-RateLimit-Remain"])
  reset = int(response.headers["X-Ratelimit-Reset"])
  # window = int(response.headers["X-Ratelimit-Window"])

  # Calculate how long to wait before making another request
  wait_time = reset - time.time()# + window

  if wait_time > 0:
    # Wait before making another request
    time.sleep(wait_time)

async def scrape_data(zipcode):
    data = {'Name': [], 'Address': [], 'Contact Person': [], 'Overdue': [], 'Url Link': []}

    async with aiohttp.ClientSession() as session:
        for pageNo in range(1, 21):
            url = f"https://find-and-update.company-information.service.gov.uk/search/companies?q={zipcode}&page={pageNo}"
            async with session.get(url) as response:
                if response.status == 200:
                    print("PageNo: ",pageNo)
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    try:
                        allcompanieslink = soup.find("div", id="search-container").find("div", class_="column-full-width").find(
                            "div", class_="grid-row").find("div", class_="column-two-thirds").find("article",
                                                                                                    id="services-information-results").find(
                            "ul", id="results").find_all("li")
                        for company in allcompanieslink:
                            companylink = company.find("a")["href"]
                            dissolved = company.find("p").get_text().__contains__("Dissolved")
                            registered = company.find("p").get_text().__contains__("Registered")
                            if not dissolved:  # skip dissolved accounts
                                companyName = company.find("a").get_text().replace("         ", " ").replace("\n",
                                                                                                              " ").replace(
                                    "      ", " ")
                                companylink = "https://find-and-update.company-information.service.gov.uk" + companylink
                                async with session.get(companylink) as resultResponse:
                                    if resultResponse.status == 200:
                                        resultList = BeautifulSoup(await resultResponse.text(), 'html.parser')
                                        itemlist = resultList.find("div", id="content-container").find("div", class_="govuk-tabs").find(
                                            "ul", class_="govuk-tabs__list").find_all("li")
                                        companyoverView = "https://find-and-update.company-information.service.gov.uk" + \
                                                          itemlist[0].find("a")["href"]
                                        try:
                                            if len(itemlist) >= 2:
                                                companyPeople = "https://find-and-update.company-information.service.gov.uk" + \
                                                                itemlist[2].find("a")["href"]
                                        except IndexError:
                                            continue

                                        async with session.get(companyoverView) as companyResponse:
                                            if companyResponse.status == 200:
                                                company_soup = BeautifulSoup(await companyResponse.text(), 'html.parser')
                                                tabList = company_soup.find("div", id="content-container").find("div",
                                                                                                                 class_="govuk-tabs").find(
                                                    "ul", class_="govuk-tabs__list").find_all("li")
                                                detailsBox = company_soup.find("div", class_="govuk-tabs__panel")
                                                officeAddress = detailsBox.find("dd").get_text().replace("\n", " ")
                                                AccountOverview = "https://find-and-update.company-information.service.gov.uk" + \
                                                                   tabList[0].find("a")["href"]

                                                async with session.get(AccountOverview) as result:
                                                    if result.status == 200:
                                                        soup = BeautifulSoup(await result.text(), 'html.parser')
                                                        if not registered:
                                                            AccountStatus = soup.find("div", id="content-container").find("div",
                                                                                                                          class_="govuk-tabs").find(
                                                                "div", class_="govuk-tabs__panel").find_all("div", class_="grid-row")
                                                            try:
                                                                AccountOverdue = AccountStatus[2].find("div", class_="column-half").find(
                                                                "h2").get_text()
                                                            except IndexError:
                                                                continue
                                                        else:
                                                            AccountStatus = soup.find("div", id="content-container").find("div",
                                                                                                                          class_="govuk-tabs").find(
                                                                "div", class_="govuk-tabs__panel").find_all("div", class_="grid-row")
                                                            AccountOverdue = AccountStatus[2].find("h2").get_text()
                                                        if "overdue" in AccountOverdue:  # Only overdue Accounts details will be saved
                                                            AccountDate = AccountStatus[2].find("div", class_="column-half").find("p").get_text()
                                                            AccountDate = AccountDate.replace("         ", " ").replace("\n", " ").replace(
                                                                "      ", " ")
                                                            if "2022" in AccountDate or "2023" in AccountDate:  # only 2022/2023 records
                                                                async with session.get(companyPeople) as response:
                                                                    if response.status == 200:
                                                                        companyContact_soup = BeautifulSoup(await response.text(), 'html.parser')
                                                                        cp=""
                                                                        detailsBox = companyContact_soup.find("div", class_="govuk-tabs__panel")
                                                                        outerDiv = companyContact_soup.find("div", class_="appointments-list")
                                                                        companyContactPerson = outerDiv.find_all("div")
                                                                        for person in companyContactPerson:
                                                                            if(person.find("a") is not None):
                                                                                if(person.find("div",class_="grid-row").find("span").get_text() == "Active"):
                                                                                    cp+=person.find("a").get_text()+" "
                                                                        contactperson=cp 
                                                                        print("overdue")           
                                                                        data["Overdue"].append(AccountDate)
                                                                        data["Name"].append(companyName)
                                                                        data["Address"].append(officeAddress)
                                                                        data["Url Link"].append(companylink)        
                                                                        data["Contact Person"].append(contactperson)
                    except AttributeError:
                        print("")
    return data

def get_overdue_accounts(data):
  overdue_accounts = []
  for i in range(len(data['Name'])):
      account = {
          'Name': data['Name'][i],
          'Address': data['Address'][i],
          'Contact Person': data['Contact Person'][i],
          'Overdue': data['Overdue'][i],
          'Url Link': data['Url Link'][i]
      }
      overdue_accounts.append(account)

  return overdue_accounts
  
@app.route('/download', methods=['POST'])
def download():
    download_option = request.form['download_option']
    filename = f"{download_option}.xlsx"
    if os.path.exists(filename):
        return send_file(filename, as_attachment=True)
    else:
        return "File Not Found."
      
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

def check_company_overdue(company_number):
  # print(company_number)
  result = {}
  available = False
  resp = requests.get(BASE_URL + "/company/{0}".format(company_number), auth=(API_KEY, ''))
  if (resp.status_code == 200):
    company_profile = resp.json()
    result['CompanyName'] = ''
    result['Address'] = ''
    result['CompanyNumber'] = ''
    result['ActiveDirector'] = ''
    result['ActiveSecretary'] = ''
    result['AccountsDue'] = ''
    result['ConfirmDue'] = ''
    result['URL'] = ''
    
    if 'accounts' in company_profile and 'overdue' in company_profile['accounts'] and company_profile['accounts']['overdue'] == True:
      if 'last_accounts' in company_profile['accounts'] and company_profile['accounts']['last_accounts']['type'] != 'null':
        if check_year(company_profile['accounts']['next_made_up_to']) == False:
          return None
        if check_year(company_profile['accounts']['next_accounts']['due_on']) == False:
          return None
        if check_year(company_profile['accounts']['last_accounts']['made_up_to']) == False:
          return None
        result['AccountsDue'] = 'Next accounts made up to ' + convert_date(company_profile['accounts']['next_made_up_to']) + ' due by ' + convert_date(company_profile['accounts']['next_accounts']['due_on'])
        result['AccountsDue'] += "  " + "Last accounts made up to " + convert_date(company_profile['accounts']['last_accounts']['made_up_to'])
        available = True
      else:
        if check_year(company_profile['accounts']['next_made_up_to']) == False:
          return None
        if check_year(company_profile['accounts']['next_due']) == False:
          return None
        result['AccountsDue'] = "First accounts made up to " + convert_date(company_profile['accounts']['next_made_up_to']) + ' due by ' + convert_date(company_profile['accounts']['next_due'])
        available = True
        
      # print(company_profile)
    
    if 'confirmation_statement' in company_profile and 'overdue' in company_profile['confirmation_statement'] and company_profile['confirmation_statement']['overdue'] == True:
      if check_year(company_profile['confirmation_statement']['next_made_up_to']) == False:
        return None
      if check_year(company_profile['confirmation_statement']['next_due']) == False:
        return None
      result['ConfirmDue'] = 'Next statement date ' + convert_date(company_profile['confirmation_statement']['next_made_up_to']) + ' due by ' + convert_date(company_profile['confirmation_statement']['next_due'])
      available = True
      if 'last_made_up_to' in company_profile['confirmation_statement']:
        if check_year(company_profile['confirmation_statement']['last_made_up_to']) == False:
          return None
        result['ConfirmDue'] += "  " + "Last statement dated " + convert_date(company_profile['confirmation_statement']['last_made_up_to'])
    
    if available:
      result['CompanyName'] = company_profile['company_name']
      result['Address'] = ''
      if 'address_line_1' in company_profile['registered_office_address']:
        result['Address'] += company_profile['registered_office_address']['address_line_1'] + ", "
      
      if 'address_line_2' in company_profile['registered_office_address']:
        result['Address'] += company_profile['registered_office_address']['address_line_2'] + ", "
      
      if 'locality' in company_profile['registered_office_address']:
        result['Address'] += company_profile['registered_office_address']['locality'] + ", "
      
      if 'region' in company_profile['registered_office_address']:
        result['Address'] += company_profile['registered_office_address']['region'] + ", "

      if 'country' in company_profile['registered_office_address']:
        result['Address'] += company_profile['registered_office_address']['country'] + ", "
          
      if 'postal_code' in company_profile['registered_office_address']:
        result['Address'] += company_profile['registered_office_address']['postal_code'] + ", "
      
      if result['Address'] != '':
        result['Address'] = result['Address'][:-2]
          
      result['CompanyNumber'] = company_profile['company_number']
      
      if 'links' in company_profile and 'officers' in company_profile['links']:
        resp = requests.get(BASE_URL + company_profile['links']['officers'], auth=(API_KEY, ''))
        while(True):
          if resp.status_code == 200:
            items = resp.json()['items']
            for item in items:
              if item['officer_role'] == 'director':
                result['ActiveDirector'] += item['name'] + ', '
              if item['officer_role'] == 'secretary':
                result['ActiveSecretary'] += item['name'] + ', '
            
            if result['ActiveDirector'] != '':
              result['ActiveDirector'] = result['ActiveDirector'][:-2]
            if result['ActiveSecretary'] != '':
              result['ActiveSecretary'] = result['ActiveSecretary'][:-2]
            break
          elif resp.status_code == 429:
            process_429_response(resp)
          else:
            print(resp.status_code)
            break
      result['URL'] = 'https://find-and-update.company-information.service.gov.uk/company/{0}'.format(result['CompanyNumber'])
      return result
  elif resp.status_code == 429:
    process_429_response(resp)
    check_company_overdue(company_number)
  else:
    print(resp.status_code)
  return None

async def check_company_with_link_for_postcode(company_link, item):
  company_data = {}
  company_data['CompanyName'] = ''
  company_data['Address'] = ''
  company_data['CompanyNumber'] = ''
  company_data['ActiveDirector'] = ''
  company_data['ActiveSecretary'] = ''
  company_data['AccountsDue'] = ''
  company_data['ConfirmDue'] = ''
  company_data['URL'] = ''
  async with aiohttp.ClientSession() as session:
    company_link = "https://find-and-update.company-information.service.gov.uk" + company_link
    async with session.get(company_link) as result:
      if result.status == 200:
        soup = BeautifulSoup(await result.text(), 'html.parser')
        Address = soup.find("div", id="content-container").find("div", class_="govuk-tabs").find(
            "div", class_="govuk-tabs__panel").find('dl').find('dd', class_="text data").get_text()
        AccountStatus = soup.find("div", id="content-container").find("div",
                                                                      class_="govuk-tabs").find(
            "div", class_="govuk-tabs__panel").find_all("div", class_="grid-row")
        if AccountStatus[2].find("h2") == None:
          return None
        OverDue = AccountStatus[2].find_all("div", class_="column-half")
        if len(OverDue) > 0:
          AccountOverdue = OverDue[0].find("h2").get_text()
          if "overdue" in AccountOverdue:
            AccountDate = AccountStatus[2].find("div", class_="column-half").find("p").get_text()
            AccountDate = AccountDate.replace("         ", " ").replace("\n", " ").replace(
                                                                  "      ", " ")
            if "2023" in AccountDate or "2024" in AccountDate:
              company_data['AccountsDue'] = AccountDate
        if len(OverDue) > 1 and OverDue[1] != None and OverDue[1].find("h2") != None:
          ConfirmDue = OverDue[1].find("h2").get_text()
          if "overdue" in ConfirmDue:
            ConfirmDate = OverDue[1].find("p").get_text()
            ConfirmDate = ConfirmDate.replace("         ", " ").replace("\n", " ").replace(
                                                                "      ", " ")
            if "2023" in ConfirmDate or "2024" in ConfirmDate:
              company_data['ConfirmDue'] = ConfirmDate

    if company_data['AccountsDue'] != '' or company_data['ConfirmDue'] != '':
      company_data['CompanyName'] = item['title']
      company_data['Address'] = Address
      company_data['CompanyNumber'] = item['company_number']
      company_data['URL'] = company_link
      if 'links' in item and 'self' in item['links']:
        resp = requests.get(BASE_URL + item['links']['self'] + '/officers', auth=(API_KEY, ''))
        while(True):
          if resp.status_code == 200:
            officers = resp.json()['items']
            for officer in officers:
              if officer['officer_role'] == 'director':
                company_data['ActiveDirector'] += officer['name'] + ', '
              if officer['officer_role'] == 'secretary':
                company_data['ActiveSecretary'] += officer['name'] + ', '
            
            if company_data['ActiveDirector'] != '':
              company_data['ActiveDirector'] = company_data['ActiveDirector'][:-2]
            if company_data['ActiveSecretary'] != '':
              company_data['ActiveSecretary'] = company_data['ActiveSecretary'][:-2]
            break
          elif resp.status_code == 429:
            process_429_response(resp)
          else:
            print(resp.status_code)
            break
      return company_data
    else:
      company_data = None
  return company_data

async def check_company_with_link(company_link, item):
  company_data = {}
  company_data['CompanyName'] = ''
  company_data['Address'] = ''
  company_data['CompanyNumber'] = ''
  company_data['ActiveDirector'] = ''
  company_data['ActiveSecretary'] = ''
  company_data['AccountsDue'] = ''
  company_data['ConfirmDue'] = ''
  company_data['URL'] = ''
  async with aiohttp.ClientSession() as session:
    company_link = "https://find-and-update.company-information.service.gov.uk" + company_link
    async with session.get(company_link) as result:
      if result.status == 200:
        soup = BeautifulSoup(await result.text(), 'html.parser')
        Address = soup.find("div", id="content-container").find("div", class_="govuk-tabs").find(
            "div", class_="govuk-tabs__panel").find('dl').find('dd', class_="text data").get_text()
        AccountStatus = soup.find("div", id="content-container").find("div",
                                                                      class_="govuk-tabs").find(
            "div", class_="govuk-tabs__panel").find_all("div", class_="grid-row")
        if AccountStatus[2].find("h2") == None:
          return None
        OverDue = AccountStatus[2].find_all("div", class_="column-half")
        if len(OverDue) > 0:
          AccountOverdue = OverDue[0].find("h2").get_text()
          if "overdue" in AccountOverdue:
            AccountDate = AccountStatus[2].find("div", class_="column-half").find("p").get_text()
            AccountDate = AccountDate.replace("         ", " ").replace("\n", " ").replace(
                                                                  "      ", " ")
            if "2023" in AccountDate or "2024" in AccountDate:
              company_data['AccountsDue'] = AccountDate
        if len(OverDue) > 1 and OverDue[1] != None and OverDue[1].find("h2") != None:
          ConfirmDue = OverDue[1].find("h2").get_text()
          if "overdue" in ConfirmDue:
            ConfirmDate = OverDue[1].find("p").get_text()
            ConfirmDate = ConfirmDate.replace("         ", " ").replace("\n", " ").replace(
                                                                "      ", " ")
            if "2023" in ConfirmDate or "2024" in ConfirmDate:
              company_data['ConfirmDue'] = ConfirmDate

    if company_data['AccountsDue'] != '' or company_data['ConfirmDue'] != '':
      company_data['CompanyName'] = item['company_name']
      company_data['Address'] = Address
      company_data['CompanyNumber'] = item['company_number']
      company_data['URL'] = company_link
      if 'links' in item and 'company_profile' in item['links']:
        resp = requests.get(BASE_URL + item['links']['company_profile'] + '/officers', auth=(API_KEY, ''))
        while(True):
          if resp.status_code == 200:
            officers = resp.json()['items']
            for officer in officers:
              if officer['officer_role'] == 'director':
                company_data['ActiveDirector'] += officer['name'] + ', '
              if officer['officer_role'] == 'secretary':
                company_data['ActiveSecretary'] += officer['name'] + ', '
            
            if company_data['ActiveDirector'] != '':
              company_data['ActiveDirector'] = company_data['ActiveDirector'][:-2]
            if company_data['ActiveSecretary'] != '':
              company_data['ActiveSecretary'] = company_data['ActiveSecretary'][:-2]
            break
          elif resp.status_code == 429:
            process_429_response(resp)
          else:
            print(resp.status_code)
            break
      return company_data
    else:
      company_data = None
  return company_data
    
    
def search_company_by_locality(locality):
  size = 200
  search_result = []
  is_end = 0
  global company_index
  start_index = company_index
  print("Search start index = {0}".format(start_index))
  
  while(True):
    params = {"location" : locality, "company_status": "active", "size": size, "start_index": start_index}
    print(params)
    resp = requests.get(BASE_URL + "/advanced-search/companies", params, auth=(API_KEY, ''))
    if resp.status_code == 200:
      items = resp.json()['items']
      for item in items:
        # print(item)
        company_index = company_index + 1
        if 'registered_office_address' in item and 'locality' in item['registered_office_address'] and item['registered_office_address']['locality'] == locality:
          # check_result = check_company_overdue(item['company_number'])
          if 'links' in item and 'company_profile' in item['links']:
            check_result = loop.run_until_complete(check_company_with_link(item['links']['company_profile'], item))
            if check_result:
              # print(check_result)
              search_result.append(check_result)
              # print("Found {0}".format(len(search_result)))
              if len(search_result) == PAGE_COUNT:
                return search_result
      if len(items) < size:
        is_end = 1
        break
      start_index += size
    elif resp.status_code == 429:
      process_429_response(resp)
    else:
      print("Advanced Search Companies Error {0}".format(resp.status_code))
      break
  return search_result
  # return

def search_company_by_postal_code(postcode):
  size = 100
  search_result = []
  global is_end
  is_end = 0
  global company_index
  start_index = company_index
  print("Search start index = {0}".format(start_index))
  
  while(True):
    params = {"q" : postcode, "items_per_page": size, "start_index": start_index}
    print(params)
    resp = requests.get(BASE_URL + "/search/companies", params, auth=(API_KEY, ''))
    if resp.status_code == 200:
      items = resp.json()['items']
      for item in items:
        # print(item)
        company_index = company_index + 1
        if 'address' in item and 'postal_code' in item['address'] and item['address']['postal_code'].strip() == postcode.strip() and item['company_status'] == 'active':
          # check_result = check_company_overdue(item['company_number'])
          if 'links' in item and 'self' in item['links']:
            check_result = loop.run_until_complete(check_company_with_link_for_postcode(item['links']['self'], item))
            if check_result:
              # print(check_result)
              search_result.append(check_result)
              # print("Found {0}".format(len(search_result)))
              if len(search_result) == PAGE_COUNT:
                return search_result
      if len(items) < size:
        is_end = 1
        print('is_end')
        break
      start_index += size
    elif resp.status_code == 429:
      process_429_response(resp)
    else:
      print("Advanced Search Companies Error {0}".format(resp.status_code))
      break
  return search_result

@app.route('/', methods=['GET', 'POST'])
def index():
  global company_index
  global is_end
  if request.method == 'POST':
      select_search_type = request.form['select_search_type']
      cur_page = int(request.form['cur_page'])
      search_action = request.form['search_action']
      if search_action == 'new':
        total_result.clear()
        company_index = 0
        cur_page = 0
      print("Page {0}".format(cur_page))
      print("Total Result {0}".format(len(total_result)))
      
      if len(total_result) > 0 and len(total_result) > (cur_page * PAGE_COUNT) and len(total_result) <= ((cur_page + 1) * PAGE_COUNT):
        data = total_result[(cur_page * PAGE_COUNT):((cur_page + 1) * PAGE_COUNT)]
        return render_template('index.html', overdue_accounts=data, is_end = is_end, search_started = 1, cur_page = cur_page, input_option = Markup(request.form["input_option"].replace(" ", "&nbsp;")), select_option = select_search_type)
      
      if select_search_type == "postal_code":
        data = search_company_by_postal_code(request.form["input_option"].replace('\xa0', ' ', 1))
      else:
        data = search_company_by_locality(request.form["input_option"].replace('\xa0', ' ', 1))

      print("Searched {0} companies. Current company index = {1}".format(len(data), company_index))
      # print(data)
      if len(data) > 0:
        total_result.extend(data)
        filename = request.form["input_option"].replace('\xa0', ' ', 1) + ".xlsx"
        df = pd.DataFrame.from_dict(total_result)
        df.to_excel(filename, index=False)
        return render_template('index.html', overdue_accounts=data, is_end = is_end, search_started = 1, cur_page = cur_page, input_option = Markup(request.form["input_option"].replace(" ", "&nbsp;")), select_option = select_search_type)
       
      # print(data)
      
      # Perform web scraping
      # data = loop.run_until_complete(scrape_data(zipcode))

      # if data['Overdue']:
      #     filename = f"{zipcode}.xlsx"
      #     df = pd.DataFrame.from_dict(data)
      #     df.to_excel(filename, index=False)
      #     overdue_accounts = get_overdue_accounts(data)  # Get overdue accounts details
      #     return render_template('index.html', overdue_accounts=overdue_accounts)
      # else:
      if len(total_result) > 0:
        return render_template('index.html', is_end = is_end, search_started = 1, cur_page = cur_page, input_option = Markup(request.form["input_option"].replace(" ", "&nbsp;")), select_option = select_search_type)
      else:
        return render_template('index.html', is_end = is_end, search_started = 0, cur_page = cur_page, input_option = Markup(request.form["input_option"].replace(" ", "&nbsp;")), select_option = select_search_type)
  company_index = 0
  total_result.clear()
  return render_template('index.html', is_end = is_end, search_started = 0, cur_page = 0, input_option = "", select_option = "locality")

if __name__ == '__main__':
  app.run()
