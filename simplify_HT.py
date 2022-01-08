from http.server import BaseHTTPRequestHandler, HTTPServer
import time
import datetime
import pandas as pd
import pymongo
import requests
import json
from pymongo import errors

hostName = "localhost"
serverPort = 8000

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes("<html><head><title></title></head>", "utf-8"))
        self.wfile.write(bytes("<p>Status ok</p>", "utf-8"))
        self.wfile.write(bytes("</body></html>", "utf-8"))
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["mydatabase"]
        coll1 = mydb["yearly_data"]
        coll2 = mydb["uni_info"]
        #locate/change the new data location here
        url1 = 'https://raw.githubusercontent.com/VentesWorks/education-dp-exercise/simplified/data-2018.txt'
        #url2 = 'https://raw.githubusercontent.com/VentesWorks/education-dp-exercise/simplified/data-2019.txt'
        #url3 = 'https://raw.githubusercontent.com/VentesWorks/education-dp-exercise/simplified/data-2020.txt'
        #print(myclient.list_database_names())
        #print(mydb.list_collection_names())
        df1=pd.read_csv(url1, sep='\t', dtype='unicode')
        col_name = list(df1)
        col_name1 = list(df1)
        #clean data
        if len(col_name) > 9:
            new_df1 = df1.replace('- ',0)
            df2 = new_df1.replace('> 1000',1001)    
        else:
            df2 = df1.replace('-',0)
        column_names = ["Institution ", "Info ", "Description "]
        col_name.append("Year")
        df6 = pd.DataFrame(columns = col_name)
        for co in col_name1:
            df6[co] = df2[co]
        for index, row in df6.iterrows():
            xx = datetime.datetime.now()
            row['Year'] = xx.year
        df3 = pd.DataFrame(columns = column_names)
        df3["Institution "] = df2["Institution "]        
        for index, row in df3.iterrows():    
            nn = row['Institution '] 
            nn1 = nn.replace('&',' and ')
            nn2 = nn1.capitalize()    
            take_url = requests.get('https://api.duckduckgo.com/?q={}&format=json&pretty=1'.format(nn2))
            take_url.raise_for_status()  # raises exception when not a 2xx response
            if take_url.status_code != 204:
                #return response.json()
                y = take_url.json()
                row['Info '] = y['AbstractURL']
                row['Description '] = y['Abstract']
                #print(df3.head(80))
                del nn 
                del take_url
                del y                
        collist = mydb.list_collection_names()
        if "uni_info" and "yearly_data" in collist:
            coll2.drop()
            mydb.uni_info.insert_many(df3.to_dict('records'))
            #mycol = mydb["uni_info"]    
            #df5 = pd.DataFrame(list(coll1.find())) 
            #print(df5)
            yy = datetime.datetime.now()
            for index, row in df6.iterrows():
                #print(dict(row))
                myquery = { "Year": yy.year, "Institution ": row["Institution " ]}
                mydoc = coll1.find(myquery)
                result = list(mydoc)
                if len(result)==0:
                    mydb.yearly_data.insert_one(dict(row))
                    print('Data Added')
                else:
                    mydb.yearly_data.update_one(myquery,{ "$set": dict(row) })
                    print('Data Updated') 
        else:
            mydb.yearly_data.insert_many(df6.to_dict('records'))
            mydb.uni_info.insert_many(df3.to_dict('records'))
if __name__ == "__main__":        
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
