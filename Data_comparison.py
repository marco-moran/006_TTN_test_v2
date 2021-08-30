import pandas as pd
from pandas.plotting import lag_plot
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import glob
import matplotlib.ticker as plticker
from datetime import datetime
import numpy as np
import configparser


config = configparser.ConfigParser()
config.read('config.ini')


data = pd.DataFrame()
data_ts = pd.DataFrame()
for file_name in glob.glob(config['data_comparison']['folder_with_input'] + '20??_??_??_??_??_??_*.csv'):
    d = pd.read_csv(file_name)
    d = d.drop(columns=['Unnamed: 0'])
    d = d.set_index("DateTime")
    d = pd.DataFrame(d[config['variable_analyzed']['variable']])
    d.index = pd.to_datetime(d.index)
    data_ts = pd.concat([data_ts, d], axis=0)
    d.columns = [d.index[0].strftime('%y-%m-%d')]
    d.index = d.index.strftime('%H:%M:%S')
    data = pd.concat([data, d], axis=1)


# for save dataframe as .png
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


def comp_analysis(df, df_ts):
    date = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')

    comparative_plot(df, date)
    time_series_plot(df_ts, df, date)
    scatter_corr(df_ts, date)
    describe_add_comp(df, date)
    hist_box_comp(df, date)
    top_v_range_comp(df_ts, date)


def comparative_plot(df, date):

    x = range(len(df.columns))

    plt.figure(figsize=(16, 5))

    for n in x:
        plt.plot(pd.to_datetime(df.index), df.iloc[:, n], label=df.columns[n])

    ax = plt.gca()
    legend = ax.legend(loc='upper center', shadow=True, fontsize='x-large')
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=60))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M'))
    plt.gcf().autofmt_xdate()  # Rotation
    plt.savefig(config['data_comparison']['path_output'] + date + "comparative_plot.png", facecolor='w', bbox_inches='tight')


def time_series_plot(df_ts, df, date):

    x = df_ts.index.strftime("%d %H:%M")
    y = df_ts

    tick_spacing = 240
    datad = len(df.index)

    fig, ax = plt.subplots(1, 1, figsize=(16, 5))
    ax.plot(x, y)
    for n in range((len(df.columns))):
        ax.plot([datad*n, datad*n], [df_ts.min(), df_ts.max()], alpha=0.3, linestyle='dashed')

    rolling = df_ts.rolling(window=60)
    rolling_mean = rolling.mean()
    ax.plot(x, rolling_mean, color="red")

    plt.xticks(rotation=45, horizontalalignment='right')
    ax.xaxis.set_major_locator(plticker.MultipleLocator(tick_spacing))

    plt.savefig(config['data_comparison']['path_output'] + date + "time_series_plot.png", facecolor='w', bbox_inches='tight')


# create a scatter plot
def scatter_corr(df_ts, date):

    plt.figure(figsize=(16, 5))
    lag_plot(df_ts)
    plt.savefig(config['data_comparison']['path_output'] + date + "scatter_corr.png", facecolor='w', bbox_inches='tight')


def describe_add_comp(df, date):
    sum_df = pd.DataFrame(df.sum(), columns=['sum']).T
    median_df = pd.DataFrame(df.median(), columns=['median']).T
    mode_df = pd.DataFrame(df.mode()[0:1]).rename(index={0: 'mode'})
    descr_df = pd.concat([pd.DataFrame(df.describe()[0:1]), mode_df, sum_df, pd.DataFrame(df.describe()[1:3]), median_df, pd.DataFrame(df.describe()[3:])])
    render_mpl_table(descr_df, header_columns=1, col_width=2.0, path=config['data_comparison']['path_output'] + date + describe_add_comp.__name__)


def top_v_range_comp(df, date):
    from datetime import datetime

    t_range = pd.DataFrame([df.index[0], df.index[-1], df.index[-1]-df.index[0]], index=["start", "end", "range"], columns=['DateTime'])
    min_v = pd.DataFrame(df.sort_values(by=df.columns[0], ascending=True).head(5))
    max_v = pd.DataFrame(df.sort_values(by=df.columns[0], ascending=False).head(5))
    render_mpl_table(t_range, header_columns=1, col_width=4.0, path=config['data_comparison']['path_output'] + date + "t_range_comp")
    render_mpl_table(min_v, header_columns=1, col_width=4.0, path=config['data_comparison']['path_output'] + date + "min_v_comp")
    render_mpl_table(max_v, header_columns=1, col_width=4.0, path=config['data_comparison']['path_output'] + date + "max_v_comp")


def hist_box_comp(df, date):
    num = len(df.columns)
    x = range(num)
    fig, ax = plt.subplots(figsize=(16, (num+1)*5), nrows=num+1)
    ax[0].boxplot(df)
    for n in x:
        ax[n+1].hist(df.iloc[:, n], bins=30, range=(df.min().min(), df.max().max()), label=df.columns[n])
        ax[n+1].legend()
    plt.savefig(config['data_comparison']['path_output'] + date + "hist_box_comp.png", facecolor='w', bbox_inches='tight')


comp_analysis(data, data_ts)

