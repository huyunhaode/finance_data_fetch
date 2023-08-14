import time
import akshare as ak
import market.a_share_stock.price.price_fetcher_info as price_fetcher_info
import market.a_share_stock.price.price_constant as price_const
import market.a_share_stock.price.price_db as price_db


def stock_code_net_to_csv():
    """
    获取A股股票代码，并保存到data/xxx.csv文件中
    新浪日行情数据需要：沪交所股票代码，代码添加前缀sh，深交所股票代码，代码添加前缀sz
    :return: [上交所DataFrame,深交所DataFrame]
    """
    stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
    # 修改股票代码前缀
    stock_sh_a_spot_em_df['代码'] = \
        stock_sh_a_spot_em_df['代码'].apply(lambda _: str(_))
    # stock_sh_a_spot_em_df['代码'].apply(lambda x: "{}{}".format('sh', x))
    # 保存
    stock_sh_a_spot_em_df.to_csv(price_const.a_share_stock_code_csv_path)

    stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
    # 修改股票代码前缀
    stock_sz_a_spot_em_df['代码'] = \
        stock_sz_a_spot_em_df['代码'].apply(lambda _: str(_))
    # stock_sz_a_spot_em_df['代码'].apply(lambda x: "{}{}".format('sz', x))
    # 保存
    stock_sz_a_spot_em_df.to_csv(price_const.a_share_stock_code_csv_path)
    return stock_sh_a_spot_em_df, stock_sz_a_spot_em_df


def start():
    """
    量化投资程序入口
    :return:
    """
    info_df = price_fetcher_info.read_info()
    # 如果配置是空，则获取沪深A股信息，获取code，保存到配置
    if info_df.empty:
        sh_df, sz_df = stock_code_net_to_csv()
        # 补全本地配置信息
        for index, row in sh_df.iterrows():
            info_df.loc[len(info_df), info_df.columns] = (row['代码'], row['名称'], price_const.start_date, 0)
        for index, row in sz_df.iterrows():
            info_df.loc[len(info_df), info_df.columns] = (row['代码'], row['名称'], price_const.start_date, 0)
        # 保存到本地
        price_fetcher_info.save_info(info_df)
        print('初始化配置信息，并保存到本地成功...')
    else:
        print('已经初始化过本地配置...')
    # 开始更新日行情数据
    update_stock_zh_a_daily_eastmoney()


def update_stock_zh_a_daily_eastmoney():
    """
    东方财富日行情数据，沪深A股，先从本地配置获取股票代码，再获取日行情数据
    获取成功或失败，记录到本地数据，以便股票数据更新完整
    :return:
    """
    success_code_list = []
    except_code_list = []
    # 读取配置信息
    info_df = price_fetcher_info.read_info()
    if info_df.empty:
        print('配置信息错误，请检查...')
        return

    for index, row in info_df.iterrows():
        code = row['code']
        start_time = row['daily_update_time']
        end_time = time.strftime('%Y%m%d', time.localtime())
        try:
            except_code = str(code)
            df = ak.stock_zh_a_hist(
                symbol=str(code),
                start_date=start_time,
                end_date=end_time,
                adjust="qfq")
        except:
            except_code_list.append(except_code)
            # 更新配置信息info_df
            info_df.loc[info_df['code'] == code, 'error_daily_update_count'] \
                = row['error_daily_update_count'] + 1

            print("发生异常code ", except_code)
            continue

        print('成功获取股票: index->{} {}日行情数据'.format(index, code), ' 开始时间: {} 结束时间: {}'.format(start_time, end_time))
        if df.empty:
            continue

        # 获取对应的子列集
        sub_df = df[['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']]
        # net_df 的列名可能和数据库列名不一样，修改列名对应数据库的列名
        sub_df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        # 修改 index 为 date 去掉默认的 index 便于直接插入数据库
        sub_df.set_index(['date'], inplace=True)
        sub_df.insert(sub_df.shape[1], 'code', str(code))
        price_db.to_table(sub_df, "a_share_stock_daily_price")
        # 更新配置信息info_df
        info_df.loc[info_df['code'] == code, 'daily_update_time'] = end_time
        info_df.loc[info_df['code'] == code, 'error_daily_update_count'] = 0
        # 间隔更新到本地配置
        if index % 100 == 0:
            price_fetcher_info.save_info(info_df)
            print('index: {} 更新本地配置一次...'.format(index))

        success_code_list.append(code)
        print(sub_df)
    # 同步配置到本地
    price_fetcher_info.save_info(info_df)

    print('更新本地配置成功...')
    print("成功请求的code： ", success_code_list)
    print("错误请求code： ", except_code_list)


if __name__ == '__main__':
    start()
    pass

