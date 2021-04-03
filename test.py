import pandas as pd
from numpy import std, mean


def ratio(main_dict, grouped_phrases, grouped_urls, group_number, type_ratio):
    if type_ratio == 1:  # если type_ratio раен 1 или не задан, то считаем l коэффициент
        n = len(grouped_phrases)  # количество фраз в группе
        ratio_dict = {'GROUP': [], 'URL': [], 'L_RATIO': []}  # финальный словарь
        for grouped_url in grouped_urls:  # для каждого сгруппированного адреса из списка сгруппированных адресов
            l_i = 0  # начальное значение значение
            for phrase in grouped_phrases:
                urls = main_dict[phrase]  # списох адресов для конкретной фразы из исходного файла
                r_i = len(urls)  # длина DataFrame по фразе
                for s_i_j, url in enumerate(urls, start=1):  # для каждого ранга, адреса в списке адресов из df_phrase
                    if grouped_url == url:  # если адрес из сгруппированного списка совпадает с адресом из исходника
                        l_i = s_i_j / r_i + l_i  # посчитать промежуточный коэффициент
            else:
                ratio_dict['GROUP'].append(group_number)  # добавление номера группы в финальный словарь
                ratio_dict['URL'].append(grouped_url)  # добавление адреса в финальный словарь
                ratio_dict['L_RATIO'].append(l_i / n)  # добавление коэффициента в финальный словарь
        else:
            return ratio_dict
    else:
        ratio_dict = {'GROUP': [], 'URL': [], 'С_V': [], 'V_R': [], 'С_V * V_R': []}  # финальный словарь
        for grouped_url in grouped_urls:  # для каждого сгруппированного адреса из списка сгруппированных адресов
            values_list = []  # список со значеними
            for phrase in grouped_phrases:
                urls = main_dict[phrase]  # списох адресов для конкретной фразы из исходного файла
                for index, url in enumerate(urls, start=1):  # для каждого ранга, адреса в списке адресов из df_phrase
                    if grouped_url == url:  # если адрес из сгруппированного списка совпадает с адресом из исходника
                        values_list.append(index)  # добавить индекс этого адреса в список
            else:
                avr = mean(values_list)  # среднее
                c_v = std(values_list) / avr  # вариация
                v_r = (max(values_list) - min(values_list)) / avr  # осцилляция
                ratio_dict['GROUP'].append(group_number)  # добавление номера группы в финальный словарь
                ratio_dict['URL'].append(grouped_url)  # добавление адреса в финальный словарь
                ratio_dict['С_V'].append(c_v)  # добавление коэффициента вариации в финальный словарь
                ratio_dict['V_R'].append(v_r)  # добавление коэффициента осцилляции в финальный словарь
                ratio_dict['С_V * V_R'].append(c_v * v_r)  # добавление произведения c_v * v_r
        else:
            return ratio_dict


main_path = 'take_bet_serp_all_kenya_nigeria_south_africa_ghana_uganda.csv'
to_grouped = 'RESULT/take_bet_serp_all_kenya_nigeria_south_africa_ghana_uganda' \
             '/take_bet_serp_all_kenya_nigeria_south_africa_ghana_uganda_grouped.csv'
main_df = pd.read_csv(main_path, sep=';')  # исходный dataframe
grouped_df = pd.read_csv(to_grouped, sep=';')  # сгрупированный dataFrame

main_phrases = list(sorted(set(main_df['PHRASES'].to_list())))  # получить список фраз
length = len(main_phrases)  # посчитать длину списка с фразами
main_dict = {}  # создать пустой именованный массив, в котором ключ – фраза, а значения – адреса
# print('Этап I/II')  # вывести этап работы
for index, phrase in enumerate(main_phrases):  # для каждой фразы и ее индекса в списке групп
    print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
    buffer_df = main_df.loc[main_df['PHRASES'] == phrase]  # создать DataFrame с url
    main_dict[phrase] = buffer_df['URL'].to_list()  # добавить запись в словарь

urls = list(sorted(set(grouped_df['URLS'].to_list())))  # список списков всех адресов
length = len(urls)  # длина списка с списками адресов групп
final_df = pd.DataFrame()

for index, urls_list in enumerate(urls):  # для каждого списка адресов группы и его индекса в списке списков адресов
    print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
    df = grouped_df.loc[grouped_df['URLS'] == urls_list]  # создать DataFrame по конкретной группе
    urls_list = urls_list.split(', ')  # создать list объект из строки с адресами
    phrases = sorted(df['PHRASES'].to_list())  # список фраз из df
    group = df.iat[0, 1]  # группа
    final_dict = ratio(main_dict, phrases, urls_list, group, )
    final_df = final_df.append(pd.DataFrame(final_dict))

print(final_df)
