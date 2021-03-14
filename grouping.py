import os
import pandas as pd


def count_main_urls(iter):
    count = 0
    for i in iter:
        if i[-1] == '/' and i.count('/') <= 3:
            count += 1
    return count


dir = os.getcwd()
csv_files = [file for file in os.listdir(dir) if file.endswith(".csv")]
result_dir = 'RESULT'

# path = (input('Укажите местоположение файла csv: '))
N = int(input('Укажите количество пересечений N: '))

if not os.path.exists(result_dir):  # если данная дирректория не существует,
    os.mkdir(result_dir)  # то ее необходимо создать

print(f'Найдено файлов csv: {len(csv_files)} ')

for file in csv_files:
    print(f'\nРабота с файлом {file}')
    df = pd.read_csv(file, sep=';')
    phrases = list(sorted(set(df['PHRASES'].to_list())))

    dic_df = {}
    length = len(phrases)
    print('Этап I/II')
    for i in phrases:
        print(f'\r{round(phrases.index(i) / length * 100, 2)}%', end='')
        buffer_df = df.loc[df['PHRASES'] == i]
        dic_df[i] = buffer_df['URL'].to_list()

    final_df = pd.DataFrame(columns=['PHRASES', 'GROUP', 'MAIN_PAGE_COUNT'])
    final_list = []
    group_num = 1

    print('\nЭтап II/II')

    for i in range(len(phrases)):
        print(f'\r{round(i / length * 100, 2)}%', end='')
        init_phrase = phrases[i]
        buffer_list = []
        first_pass_indicator = True
        try:
            for target_phrase in phrases[i + 1:]:
                # print(target_phrase)
                urls_init = dic_df[init_phrase]
                urls_target = dic_df[target_phrase]
                intersection = list(set(urls_init) & set(urls_target))
                if len(intersection) >= N:
                    count_main = count_main_urls(intersection)
                    if first_pass_indicator:
                        buffer_list.append([init_phrase, group_num, count_main])
                        buffer_list.append([target_phrase, group_num, count_main])
                        first_pass_indicator = False
                    else:
                        buffer_list.append([target_phrase, group_num, count_main])
                else:
                    continue
            else:
                if len(buffer_list) == 0:
                    continue
                else:
                    buffer_df = pd.DataFrame(buffer_list, columns=['PHRASES', 'GROUP', 'MAIN_PAGE_COUNT'])
                    min_main = min(buffer_df['MAIN_PAGE_COUNT'])
                    for index in buffer_df.index:
                        buffer_df.at[index, 'MAIN_PAGE_COUNT'] = min_main
                    final_df = final_df.append(buffer_df)
                    group_num += 1

        except IndexError:
            break

    file_dir = file[:-4]
    if not os.path.exists(f'{result_dir}/{file_dir}'):  # если данная дирректория не существует,
        os.mkdir(f'{result_dir}/{file_dir}')  # то ее необходимо создать

    final_df = final_df.reset_index(drop=True)
    final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_grouped.csv', sep=';', index=False)

    phrases_after = sorted(set(final_df['PHRASES'].to_list()))
    diff_list = list(set(phrases) - set(phrases_after))
    diff_df = df[df['PHRASES'].isin(diff_list)]
    diff_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_rest.csv', sep=';', index=False)

print('\nDONE!')
