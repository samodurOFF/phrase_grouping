import pandas as pd
from itertools import combinations


def count_main_urls(iter):
    count = 0
    for i in iter:
        if i[-1] == '/' and i.count('/') <= 3:
            count += 1
    return count


df = pd.read_csv('take_bet_serp_all_kenya_nigeria_south_africa_ghana_uganda.csv', sep=';')
# urls = (set(df['URL'].to_list()))
# print(len(urls))
phrases = list(sorted(set(df['PHRASES'].to_list())))
print(len(phrases))
# print(len(phrases))
N = 5

dic_df = {}
for i in phrases:
    buffer_df = df.loc[df['PHRASES'] == i]
    dic_df[i] = buffer_df['URL'].to_list()
    # print(phrases.index(i))



final_df = pd.DataFrame(columns=['PHRASES', 'GROUP', 'MAIN_PAGE_COUNT'])
# group_num = 1

'''for i in range(len(phrases)):
    init_phrase = phrases[i]
    print(init_phrase)
    buffer_list = []
    buffer_list.append([init_phrase, group_num])
    for target_phrase in phrases[i + 1:]:
        # print(target_phrase)
        df_1 = df.loc[df['PHRASES'] == init_phrase]
        list_by_url_1 = df_1['URL'].to_list()
        df_2 = df.loc[df['PHRASES'] == target_phrase]
        list_by_url_2 = df_2['URL'].to_list()
        intersection = list(set(list_by_url_1) & set(list_by_url_2))
        if len(intersection) >= N:
            buffer_list.append([target_phrase, group_num, count_main_urls(intersection)])
            print(buffer_list)
        else:
            continue
    else:
        if len(buffer_list) == 1:
            continue
        else:
            buffer_df = pd.DataFrame(buffer_list)
            min_main = min(buffer_df[2][1:])
            for index, main_page_count in enumerate(buffer_df[2]):
                buffer_df.at[index, main_page_count] = min_main

            final_df = final_df.append(buffer_df)
            group_num += 1
'''
# combs = list(combinations(urls, N))
# print(combs)
# print(final_df)
# final_df.to_csv('grouped_phrases.csv', sep=';')
