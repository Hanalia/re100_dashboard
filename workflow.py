## workflow

import requests
from datetime import datetime
import pandas as pd


## 결과물은 3가지
# 0) data = 1주일마다 크롤링한 모든 데이터가 있음
# 1) recent_data = 현재 사이트에 있는 최신 시점의 표
# 2) updates_data = 1주 전과의 차이


# 1) 먼저 파일을 불러와서 origin_df로 저장

origin_df = pd.read_json("./src/mydata/data.json", orient='records')

# 합칠 때 에러 방지를 위해 Date은 String으로 
origin_df['Date'] =pd.to_datetime(origin_df['Date'], format='%Y-%m-%d')



# 2) 크롤링하고 new_df에 저장


try:
    url = 'https://www.there100.org/views/ajax?_wrapper_format=drupal_ajax'
    headers = {
'User=Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
'Content-type':'application/x-www-form-urlencoded; charset=UTF-8',
'Accept':'application/json, text/javascript, */*; q=0.01',
'Origin':'https://www.there100.org',
'Referer':'https://www.there100.org/re100-members'}
    dfs = []
    for page in range(0,100):
        
        data = f'view_name=members&view_display_id=block&view_args=&view_path=%2Fnode%2F149&view_base_path=&view_dom_id=8b3713037a4a73a1f9a879e8e9111c1563aabaf492a051edafedc4d8b646835b&pager_element=0&page={page}&_drupal_ajax=1&ajax_page_state%5Btheme%5D=re100&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=climate_group%2Fcta-button%2Cclimate_group%2Fcta-text%2Cclimate_group%2Ffooter%2Cclimate_group%2Fglobal-styles%2Cclimate_group%2Fheader%2Cclimate_group%2Fhero-pages%2Cclimate_group%2Fmembers-js%2Cclimate_group%2Fnews%2Ccore%2Fpicturefill%2Ceu_cookie_compliance%2Feu_cookie_compliance_default%2Cparagraphs%2Fdrupal.paragraphs.unpublished%2Cre100%2Fglobal-styles%2Csystem%2Fbase%2Cviews%2Fviews.ajax%2Cviews%2Fviews.module'
        response = requests.post(url=url, headers=headers, data=data,allow_redirects=True).json()
        myhtml = response[2]["data"]
        df2 = pd.read_html(myhtml, encoding='utf-8')[0]


        secondpart = df2.iloc[1::2,:]   
        df2 = df2.iloc[::2,:]

        df2['Explanation'] = secondpart['Type'].values

        # website 뒤에 이상한거 없애기
        df2['Website'] = df2['Website'].str.replace(r'\.a{.*', '', regex=True)
        dfs.append(df2)

    new_df = pd.concat(dfs, ignore_index=True)
except:
    new_df = pd.concat(dfs, ignore_index=True)

# 오늘 날짜를 string type으로 추가하기
new_df['Date'] = datetime.today().strftime('%Y-%m-%d')    

# new_df는 최신 자료로 저장해두기
new_df.to_json("./src/mydata/recent_data.json", orient='records')


# 3) 변동 내역은 updated_data로 빼주기
## 이 때 변동내역의 기준 : 'Name, Type 어느 하나라도 바뀐거'
## 즉 Name과 Type 모든 것이 같은 경우만 drop 하기
cond1 = new_df['Name'].isin(origin_df['Name'])
cond2 = new_df['Type'].isin(origin_df['Name'])


updates_df = origin_df.drop(new_df[cond1 & cond2].index)
updates_df.to_json("./src/mydata/updates_data.json", orient='records')

# 4) origin df 와 new df합치고 일단 저장해두기

finaldf = pd.concat([origin_df,new_df],ignore_index=True)
finaldf.to_json("./src/mydata/data.json", orient='records')

