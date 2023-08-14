import os
import pandas as pd
import market.a_share_stock.price.price_constant as price_constant


def create_info():
    """
    配置文件参数
    code：股票代码 ，name: 股票名称
    last_update_time: 最后一次更新的日期
    error_update_count：请求更新错误次数统计，定期清理多次更新错误的股票代码
    :return: DataFrame
    """
    columns = ['code', 'name', 'daily_update_time', 'error_daily_update_count']
    df = pd.DataFrame(columns=columns)
    save_info(df)
    return df


def read_info(is_new=False):
    """
    is_new if true del config else if part update
    如果是新则删除配置信息，重置，重新更新，否则增量更新
    读取配置信息，并对返回的dataFrame 数据类型进行预处理
    :return:
    """
    if is_new:
        os.remove(price_constant.a_share_stock_price_request_info_csv_path)
        print('删除配置成功，重置配置...')

    try:
        df = pd.read_csv(price_constant.a_share_stock_price_request_info_csv_path)
    except Exception as error:
        print("创建配置信息：", error)
        df = create_info()
    # code 强制转str类型，并补全股票代码为6位
    df['code'] = df['code'].astype(str)
    df['code'] = df['code'].str.zfill(6)
    df['error_daily_update_count'] = df['error_daily_update_count'].astype(int)
    return df


def save_info(df):
    """
    保存配置信息，忽略列
    :param df:
    :return:
    """
    df.to_csv(price_constant.a_share_stock_price_request_info_csv_path, encoding='utf-8', index=False)

