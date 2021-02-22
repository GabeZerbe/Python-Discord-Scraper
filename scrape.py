import requests
import json
import time
import os, sys
import datetime 
import pyodbc

from random import choice
from time import mktime, sleep
from datetime import timedelta, datetime

#return a random string of specified length
random_str = lambda length: ''.join([choice('0123456789ABCDEF') for i in range(length)])

#Returning the 'snowflake' code from a discord time stamp and converting a timestamp back into a SF
snowflake = lambda timestamp_s: (timestamp_s * 1000 - 1420070400000) << 22
timestamp = lambda snowflake_t: ((snowflake_t >> 22) + 1420070400000) / 1000.0



#Function to get our timestamps
def get_day(day, month, year):
    """
    param day: target day
    param month: target month
    param year: target year
    """

    min_time = mktime((year, month, day, 0, 0 ,0, -1 , -1, -1))
    max_time = mktime((year, month, day, 23, 59, 59, -1, -1, -1))

    return{
        '00:00': snowflake(int(min_time)),
        '23:59': snowflake(int(max_time))
    }

def safe_name(name):
    # Converts Server/Channel names into SQL Safe names

    output = ""
    for char in name:
        if char not in '\\/<>:;"|?*\' ':
            output += char
    
    return output

def create_query_body(**kwargs):
    query  = ""

    for key, value in kwargs.items():
        if value is True:
            query += '&has=%s' % key[:-1]

    return query


#Classes for stuff

class Discord:
    #Scraper Class
    def __init__(self):
        #Scraper constructor
        with open('config.json', 'r') as f:
            config = json.load(f)

        cfg = type('DiscordConfig', (object,), config)()
        if cfg.token == "Enter User Token Here" or cfg.token is None:
            print('Please set your user token in the config file')
            exit(-1)

        self.api = cfg.API
        self.buffer = cfg.buffer 
        
        self.headers = {'user-agent': cfg.agent,
                        'authorization': cfg.token}

        self.types = cfg.types

        #This isn't really used for this version, but if you would like to query for these specific things you can add them at the end of the query string in grab_data
        self.query = create_query_body(
            images = cfg.query['images'],
            files = cfg.query['files'],
            embeds=cfg.query['embeds'],
            links=cfg.query['links'],
            videos=cfg.query['videos'],
        ) 

        self.servers = cfg.servers if len(cfg.servers) > 0 else{}

    @staticmethod
    def insert_text(server, channel, message):
        #To prevent a possible SQL injection we use paramterized SQL Strings so we need to prep our message content
        mAuthorID = message['author']['id']
        mAuthor = '%s#%s' % (message['author']['username'], message['author']['discriminator'])
        mContent = message['content']
        mTimeStamp = message['timestamp']
        mID = message['id']
 
        sname = server.replace("'", "")
        cname = channel.replace("'", "")

        #We're also going to prepepare out SQL Strings
        makeTable = f"""IF OBJECT_ID('[dbo].[{sname}-{cname}]', 'U') IS NULL
        CREATE TABLE [dbo].[{sname}-{cname}] (
            [userID] NVARCHAR(50) NOT NULL,
            [username] NVARCHAR(50) NOT NULL,
            [content] NVARCHAR(MAX) NOT NULL,
            [timestamp] NVARCHAR(50) NOT NULL,
            [messageID] NVARCHAR(200) NOT NULL UNIQUE    
        );"""

        insertData = f"""INSERT INTO [dbo].[{sname}-{cname}] ([userId], [username], [content], [timestamp], [messageID])
        VALUES(?,?,?,?,?);"""

        #Establish a connection with your Azure DB you may have to hack this function a bit if you use a different DB
        with pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server={Server Address Here},1433;Database={Your DB name Here};Uid={User ID Here};Pwd={Your Password Here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;') as conn:
            with conn.cursor() as cursor:

                cursor.execute(makeTable)
                cursor.execute(insertData, (mAuthorID, mAuthor, mContent, mTimeStamp, mID))
                
                conn.commit()

    def get_server_name(self, serverid, headers):
        #Get the server name from it's serverid
        # param: serverid is the ID set in config
        # param: headers is the headers set in __init__

        r = requests.request('GET', 'https://discordapp.com/api/%s/guilds/%s' %
                                    (self.api, serverid), headers = headers)
        server = r.json()

        if server is not None:
            return('{!r}'.format(safe_name(server['name'])))
        else:
            error:('Could not fetch server name from id, generating a random name.')
            return '%s_%s' % (serverid, random_str(12))
    
    def get_channel_name(self, channelid, headers):
        #Get the Channel(s) name
        #param channelid: channel ID from config
        #param headers: self.headers
        r = requests.request('GET', 'https://discordapp.com/api/%s/channels/%s' % 
                                (self.api, channelid), headers = headers)
        channel = r.json()

        if channel is not None and len(channel) > 0:
            return ('{!r}'.format(safe_name(channel['name'])))

    def grab_data(self, server, channel, headers):
        #Grab the data from the server and input into a DB connection

        date = datetime.today()
        sName = self.get_server_name(server, headers = self.headers)
        cName = self.get_channel_name(channel, headers = self.headers)

        #Set this date to as far back as 2015 which is the "epoch" of Discord
        while date.year >= 2015:
            today = get_day(date.day, date.month, date.year)

            r = requests.request('GET','https://discordapp.com/api/%s/guilds/%s/messages/search?channel_id=%s&min_id=%s&max_id=%s' %
                    (self.api, server, channel, today['00:00'], today['23:59']), headers = headers)
            
            content = r.json()
            print(r)

            if (r.status_code == 200): 
                try:
                    if content['messages'] is not None:
                        for messages in content['messages']:
                            for message in messages:
                                if self.types['text'] is True:
                                    if len(message['content']) > 0:
                                        print("Passing %s to the db" % message['id'])
                                        self.insert_text(sName, cName, message)
                    time.sleep(1)
                    print(date)
                    date += timedelta(days =- 1)
            
                except TypeError as e:
                    print("Error: %s" % (e))
                    continue


    def get_server_info(self):
        #Scrolls through each server and its channels, entering messages into the DB
        if servers is not None:
            for servers, channels in self.servers.items():
                for channel in channels:
                    self.grab_data(servers, channel, self.headers)
        else:
            exit(-1)


if __name__ == '__main__':
    discord = Discord()
    discord.get_server_info()