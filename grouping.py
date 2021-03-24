import os
import pandas as pd


def count_main_urls(iter):
    """
    Подсчет главных страниц
    :param iter:
    :return:
    """
    count = 0  # входное значние
    for i in iter:  # передор списка
        if i[-1] == '/' and i.count('/') <= 3:  # если url закнчивается на '/' и таким символов не более трех
            count += 1  # то увеличить count на 1
    return count


def group(phrases):
    length = len(phrases)  # посчитать длину списка с фразами
    dic_df = {}  # создать пустой именованный массив
    print('Этап I/III')  # вывести этап работы
    for phrase in phrases:  # для каждой фразы из списка фраз
        print(f'\r{round(phrases.index(phrase) / length * 100, 2)}%', end='')  # вывести процент обработанных фраз
        buffer_df = df.loc[df['PHRASES'] == phrase]  # создать DataFrame с url
        dic_df[phrase] = buffer_df['URL'].to_list()  # добавить запись в словаря, где ключ – фраза, а значени – адреса

    final_df = pd.DataFrame(columns=columns)  # пустой финальный DataFrame
    group_num = 1  # начальное значение группы

    print('\nЭтап II/III')  # вывод информации о начале второго этапа работы над файлом
    for i in range(len(phrases)):  # для каждой фразы в списке фраз
        print(f'\r{round(i / length * 100, 2)}%', end='')  # вывести процент отработанных фраз
        init_phrase = phrases[i]  # определить значение начальной фразы
        buffer_list = []  # создать пустой буфферный лист
        first_pass_indicator = True  # индикация первого прохода
        current_intersection = []  # список для хранения совпадающих адресов
        min_count_main = 0  # минимальное количество главных страниц
        try:  # обработка исключения ошибки несуществующего индекса. Должен срабатывать на последней фразе
            for target_phrase in phrases[i + 1:]:  # для каждой фразы в списке фраз без init_phrase
                # print(target_phrase) # вывести фразу
                urls_init = dic_df[init_phrase]  # получить список адресов фразы init_phrase
                urls_target = dic_df[target_phrase]  # получить список адресов target_phrase
                intersection_stage_I = list(set(urls_init) & set(urls_target))  # найти перечечение списков этих фраз
                if len(intersection_stage_I) >= N:  # если количество перечечений больше ли равно N, то
                    if not current_intersection:  # проверить пуст ли список совпадающих адресов. Если он пуст, то
                        current_intersection = intersection_stage_I  # присвоить этому списку, полученный список
                    else:  # если список совпадающих адресов не пустой, то найти пересечение с ним полученого списка
                        intersection_stage_II = list(set(current_intersection) & set(intersection_stage_I))
                        if len(intersection_stage_II) >= N:  # если количество таких пересеченй не меньше N
                            current_intersection = intersection_stage_II  # присвоить этому списку, полученный список
                        else:  # иначе пропустить
                            continue  # начать цикл заново

                    count_main = count_main_urls(current_intersection)  # посчитать главные страницы
                    if first_pass_indicator:  # если это первый проход, то
                        min_count_main = count_main  # указать что count_main минимально
                        buffer_list.append([init_phrase, group_num])  # добавить первую фразу
                        buffer_list.append([target_phrase, group_num])  # и вторую
                        first_pass_indicator = False  # отметить, что первый проход был осуществлен
                    else:  # если первый проход уже был, то
                        if count_main < min_count_main:  # если новое значение count_main меньше минимального
                            min_count_main = count_main  # задать новое значение min_count_main
                        buffer_list.append([target_phrase, group_num])  # добавить только вторую фразу
                else:  # если количество перечечений меньше N, то
                    continue  # начать цикл заново
            else:
                if len(buffer_list) == 0:  # если проход по фразе не встретил перечечений ни с одной другой фразой, то
                    continue  # начать цикл заново
                else:
                    buffer_df = pd.DataFrame(buffer_list, columns=['PHRASES', 'GROUP'])  # создать буферный DataFrame
                    buffer_df['MAIN_PAGE_COUNT'] = None  # добавление в DataFrame столбца 'MAIN_PAGE_COUNT'
                    buffer_df['URLS'] = None  # добавление в DataFrame столбца 'URLS'
                    for index in buffer_df.index:  # для каждой строки
                        buffer_df.at[index, 'MAIN_PAGE_COUNT'] = min_count_main  # задать значение главных страниц
                        buffer_df.at[index, 'URLS'] = current_intersection  # записать список с пересечениями
                    final_df = final_df.append(buffer_df)  # моместить в финальный dataFrame
                    group_num += 1  # увеличить группу на 1

        except IndexError:  # если проход по посленей фразе, то завершить цикл
            break

    return final_df


def filtration(df):
    while True:
        columns = list(df.columns)  # список с именами колонок
        final = pd.DataFrame(columns=columns)  # пустной финальный DataFrame
        groups = sorted(set(df['GROUP'].to_list()))  # список всех групп
        length = len(groups) - 1  # длина списка с группами - 1, для вывода процентов статус-бара ниже
        print('Этап III/III')  # вывести этап работы
        for index, group in enumerate(groups):  # для каждой группы из списка групп
            print(f'\r{round(index / length * 100, 2)}%', end='')  # вывести процент обработанных групп
            df_group = df.loc[df['GROUP'] == group]  # dataFrame по группе
            if groups[index] == groups[-1]:  # если это последняя группа, то
                break  # прекратить цикл

            phrases_group = df_group['PHRASES'].to_list()  # список фрах из df_group
            for next_group in groups[index + 1:]:  # для каждой группы из списка групп, за исключением уже используемой
                df_next_group = df.loc[df['GROUP'] == next_group]  # dataFrame по группе
                phrases_next_group = df_next_group['PHRASES'].to_list()  # список фрах из df_next_group
                intersection = list(set(phrases_group) & set(phrases_next_group))  # перечечение списков с фразами
                if intersection == phrases_group:  # если пересечение фраз равно спичку во внешнем цикле for, то
                    final.append(phrases_next_group)  # то добавить внутренний список в финальный DataFrame
                    break  # и завершить врутренний цикл for, чтобы перейти к следующей группе
                elif intersection == phrases_next_group:  # если пересечение равно спичку во внутреннем цикле for, то
                    final.append(phrases_group) # то добавить внешний список в финальный DataFrame
                    break # и завершить врутренний цикл for, чтобы перейти к следующей группе
                else:
                    continue # если перечечений нет, то продолжить внутренний цикл

        if len(final_df) == 0: # если после прохода циклов финальный фрэйм будет пустой, то
            break # заверншить бесконечный цикл
        else: # еслифинальный цикл не пустой, то повротить цикл заново,для повторной проверки перечечения
            df = final # присвоить исходному фрэйму финальный
            continue # и продолжить цикл

    return df


def save_rest(init_df, final_df, phrases, result_dir, file_dir):
    phrases_after = sorted(set(final_df['PHRASES'].to_list()))  # все сгруппированные фразы
    diff_list = list(set(phrases) - set(phrases_after))  # фразы, которые не были сгруппированы
    diff_df = init_df[init_df['PHRASES'].isin(diff_list)]  # сортирвоака по несгруппированным фразам
    diff_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_rest.csv', sep=';', index=False)  # запись в файл
    print(f'\nФайл с несгрупированными фразами сохранен')


if __name__ == '__main__':
    dir = os.getcwd()  # вернуть текущую папку
    csv_files = [file for file in os.listdir(dir) if file.endswith(".csv")]  # список всех .csv файлов
    result_dir = 'RESULT'  # папка для сохранения результатов
    columns = ['PHRASES', 'GROUP', 'MAIN_PAGE_COUNT', 'URLS']  # список с названиями колонок
    N = int(input('Укажите количество пересечений N: '))  # запрос у пользователя количества пересечений для поиска
    if not os.path.exists(result_dir):  # если данная дирректория не существует,
        os.mkdir(result_dir)  # то ее необходимо создать

    print(f'Найдено файлов csv: {len(csv_files)} ')  # вывод информации о количестве файлов csv в консоль
    for file in csv_files:  # для каждого файла в списке
        print(f'\nРабота с файлом {file}')  # вывести имя файла
        df = pd.read_csv(file, sep=';')  # создать DataFrame
        phrases = list(sorted(set(df['PHRASES'].to_list())))  # получить список фраз
        final_df = group(phrases)  # групировка фраз и создание final_df

        file_dir = file[:-4]  # по сути, это имя файла без расширения
        if not os.path.exists(f'{result_dir}/{file_dir}'):  # если данная дирректория не существует,
            os.mkdir(f'{result_dir}/{file_dir}')  # то ее необходимо создать

        final_df = final_df.reset_index(drop=True)  # реиндексирование
        save_rest(df, final_df, phrases, result_dir, file_dir)  # сохранение несгрупированных фраз в отделный файл

        final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_grouped.csv', sep=';', index=False)  # запись в файл
        final_df = filtration(final_df)
        final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_grouped.csv', sep=';', index=False)  # запись в файл

    print('\nDONE!')
