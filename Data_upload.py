# def for convert data unix to timestamp
def date_f(unix_d):
    d = datetime.utcfromtimestamp(unix_d).strftime('%Y-%m-%d %H:%M:%S')
    d1 = datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
    return d1


# Prepare csv for downlink and send
def downlink_http(app, webhook, dev, passw, data):
    from datetime import datetime

    x = base64.b64encode(str(data).encode("ascii")).decode("ascii")

    headers = {'Authorization': 'Bearer ' + passw + ''}
    data = '{"downlinks":[{"frm_payload":"' + x + '","f_port":15, "priority":"NORMAL"}]}'

    r = requests.post(
        'https://eu1.cloud.thethings.network/api/v3/as/applications/' + app + '/webhooks/' + webhook + '/devices/' + dev + '/down/push',
        headers=headers, data=data)

    print("URL: {}\n".format(r.url))
    print("Status: {} {}\n".format(r.status_code, r.reason))
    print("Datetime: {}\n".format(datetime.now().strftime('%Y%m%d%H%M')))
    print("Response: {}\n".format(r.text))


def data_down():
    import Data_download

def data_comp():
    import Data_comparison


if __name__ == "__main__":

    import pandas as pd
    from datetime import datetime
    import base64
    import requests
    import configparser
    import multiprocessing
    import time

    # load file config.ini for setting enviroment variables
    config = configparser.ConfigParser()
    config.read('config.ini')

    # data on kaggle
    data = pd.read_csv(config['data_upload']['path_input'])

    # select 1 device
    data = data.loc[data['device'] == 'b8:27:eb:bf:9d:51']

    # convert data unix in TimeStamp
    data['ts'] = data['ts'].apply(date_f)

    # rename ts in DateTime
    data = data.rename(columns={"ts": "DateTime"})

    # set seconds to zero and drop duplicate (to have only one value per minute) with column device, light and motion
    data['DateTime'] = data['DateTime'].apply(lambda t: t.replace(second=0))
    data = data.drop_duplicates(subset=['DateTime']).drop(columns=['device', 'light', 'motion'])

    # split dataset in 4
    df1 = data[(data['DateTime'] > '2020-07-15 23:59:00') & (data['DateTime'] < '2020-07-17 00:00:00')]
    df2 = data[(data['DateTime'] > '2020-07-16 23:59:00') & (data['DateTime'] < '2020-07-18 00:00:00')]
    df3 = data[(data['DateTime'] > '2020-07-17 23:59:00') & (data['DateTime'] < '2020-07-19 00:00:00')]
    df4 = data[(data['DateTime'] > '2020-07-18 23:59:00') & (data['DateTime'] < '2020-07-20 00:00:00')]

    # set name for every df and put in list
    df1.name = 'df1'
    df2.name = 'df2'
    df3.name = 'df3'
    df4.name = 'df4'
    lis = [df1, df2, df3, df4]


    # save the files in csv
    for x in lis:
        file = open('ts_' + x.name + '.csv', 'w', newline='')
        file.write(x.to_csv(index=False))
        file.close()

    # start first process with def data_down
    p1 = multiprocessing.Process(target=data_down)
    p1.start()

    time.sleep(5) # wait untill the processe is done

    # csv to dict/json for every df and send with def downlink_http
    for x in lis:
        ts = pd.read_csv('ts_' + x.name + '.csv')
        ts = {'payload': ts.to_dict(orient='records')}
        downlink_http(app=config['data_upload']['application'],
                      webhook=config['data_upload']['webhook'],
                      dev=config['data_upload']['device'],
                      passw=config['DEFAULT']['password_api'],
                      data=ts)

    p1.join(len(lis)*5) # timeout for process (sec)
    p1.terminate() # terminate because join does not seem work
    
    # start second process with def data_comp
    p2 = multiprocessing.Process(target=data_comp)
    p2.start()
    p2.join()
