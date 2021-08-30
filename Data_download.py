import paho.mqtt.client as mqtt
import json
import pandas as pd
import ast
from datetime import datetime
import base64
import numpy as np
import matplotlib.pyplot as plt
import configparser
from sqlalchemy import create_engine
import psycopg2


# load file config.ini for setting enviroment variables
config = configparser.ConfigParser()
config.read('config.ini')


# save dataframe into database sql
def df_tosql(df):
    engine = create_engine('postgresql+psycopg2://postgres:Drowssap11@localhost:5432/TTN')
    df.to_sql(name='ttn', con=engine, if_exists='append')
    con=psycopg2.connect(dbname="TTN", user="postgres", password="Drowssap11", port=5432)
    cur = con.cursor()          
    cur.execute("SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY' AND table_name = 'ttn'")
    rows = cur.fetchall()
    if len(rows) == 0:
        cur.execute('ALTER TABLE ttn ADD PRIMARY KEY ("DateTime")')
        con.commit()
        con.close()
    con.close()


# for convert and save the dataframes output as .png
def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=14,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, path="", **kwargs):
    data = data.round(5)
    data = data.reset_index()
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)
    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in mpl_table._cells.items():
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0] % len(row_colors)])
    ax.get_figure().savefig(path + ".png")


# describe() with additions
def describe_add(df, colname, name_file):
    s = df[colname]
    sum_d = pd.DataFrame(s.sum(), index=['sum'], columns=[colname])
    median_d = pd.DataFrame(s.median(), index=['median'], columns=[colname])
    mode_d = pd.DataFrame(s.mode()[0], index=['mode'], columns=[colname])
    descr = pd.concat([pd.DataFrame(s.describe()[0:1]), mode_d, sum_d, pd.DataFrame(s.describe()[1:3]), median_d, pd.DataFrame(s.describe()[3:])])
    render_mpl_table(descr, header_columns=1, col_width=2.0, path=config['data_download']['path_output'] + name_file + describe_add.__name__)


# histogram and boxplot
def hist_box(df, colname, name_file):
    fig, ax = plt.subplots(figsize=(16, 5), nrows=1, ncols=2)
    s = df[colname]

    ax[0].hist(s, bins=30)
    ax[0].set(title='Histogram', xlabel=colname)

    ax[1].boxplot(s)
    ax[1].set(title='Boxplot')

    plt.savefig(config['data_download']['path_output'] + name_file + "hist_box.png", facecolor='w', bbox_inches='tight')


# dataframe output of range, max 5 and min 5 values
def top_v_range(df, colname, name_file):
    from datetime import datetime

    s = df[colname]
    t_range = pd.DataFrame([df.index[0], df.index[-1], df.index[-1]-df.index[0]], index=["start", "end", "range"], columns=['DateTime'])
    min_v = pd.DataFrame(s.sort_values(ascending=True).head(5))
    max_v = pd.DataFrame(s.sort_values(ascending=False).head(5))
    render_mpl_table(t_range, header_columns=1, col_width=4.0, path=config['data_download']['path_output'] + name_file + "t_range")
    render_mpl_table(min_v, header_columns=1, col_width=4.0, path=config['data_download']['path_output'] + name_file + "min_v")
    render_mpl_table(max_v, header_columns=1, col_width=4.0, path=config['data_download']['path_output'] + name_file + "max_v")


# plot time series
def plot_d(df, colname, name_file):
    s = df[colname]
    plt.figure(figsize=(16, 5))
    plt.plot(df.index, s, color='tab:red')
    plt.xlabel(df.index.name)
    plt.ylabel(colname)
    plt.xticks(rotation=45, horizontalalignment='right')
    plt.savefig(config['data_download']['path_output'] + name_file + "plot_d.png", facecolor='w', bbox_inches='tight')

# for subcsribe to server MQTT and get downlinks and uplinks
def mqtt_sub(broker, port, appid, passw):
    def on_connect(client, userdata, flags, rc):
        print("Connected with result code " + str(rc))
        client.subscribe('v3/+/devices/+/down/failed')  # subscribe to downlinks scheduled
        client.subscribe('v3/+/devices/+/down/sent')  # subscribe to uplinks
        client.subscribe('v3/+/devices/+/up')  # subscribe to uplinks

    def on_message(client, userdata, msg):
        print(msg.topic + " " + str(msg.payload))
        on_message.counter += 1
        date = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
        if '/failed' in msg.topic:
            a = [json.loads(msg.payload.decode('utf-8'))]
            df = pd.json_normalize(a)
            print(type(df), df)
            # with open(date + '_' + str(on_message.counter) + '.csv', 'a', newline='') as f:
            #    df.to_csv(f, header=f.tell()==0)
            #    f.close()
            data = df['downlink_failed.downlink.frm_payload'][0]
            data = str(base64.b64decode(data))[14:-2]
            data = ast.literal_eval(data)
            df_data = pd.json_normalize(data)
            name_file = date + '_' + str(on_message.counter)
            with open(config['data_download']['path_output'] + name_file + '.csv', 'a', newline='') as d:
                df_data.to_csv(d, header=d.tell() == 0)
                d.close()

            df = pd.read_csv(config['data_download']['path_output'] + name_file + '.csv', index_col='DateTime').drop(labels="Unnamed: 0", axis=1)
            df = df.sort_index()
            df.index = pd.to_datetime(df.index, infer_datetime_format=True)
            
            describe_add(df=df, colname=config['variable_analyzed']['variable'], name_file=name_file)
            hist_box(df=df, colname=config['variable_analyzed']['variable'], name_file=name_file)
            top_v_range(df=df, colname=config['variable_analyzed']['variable'], name_file=name_file)
            plot_d(df=df, colname=config['variable_analyzed']['variable'], name_file=name_file)

            df_tosql(df)

        elif '/up' in msg.topic:
            name_file = date + '_' + str(on_message.counter)
            a = [json.loads(msg.payload.decode('utf-8'))]
            df = pd.json_normalize(a)
            print(type(df), df)
            with open(config['data_download']['path_output'] + name_file + '.csv', 'a', newline='') as f:
                df.to_csv(f, header=f.tell() == 0)
                f.close()

    def on_log(client, userdata, level, buf):
        print("LOG: message:" + str(buf))
        print("LOG: userdata:" + str(userdata))

    on_message.counter = 0

    client = mqtt.Client()
    client.on_log = on_log
    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set(appid, passw)
    client.connect(broker, port, 60)

    client.loop_forever()


mqtt_sub(broker=config['data_download']['broker'],
         port=int(config['data_download']['port']),
         appid=config['data_download']['application_id'],
         passw=config['DEFAULT']['password_api'])
