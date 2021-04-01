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


def group(phrases, df):
    length = len(phrases)  # посчитать длину списка с фразами
    dic_df = {}  # создать пустой именованный массив
    print('Этап I/IV')  # вывести этап работы
    for phrase in phrases:  # для каждой фразы из списка фраз
        print(f'\r{round(phrases.index(phrase) / length * 100, 2)}%', end='')  # вывести процент обработанных фраз
        buffer_df = df.loc[df['PHRASES'] == phrase]  # создать DataFrame с url
        dic_df[phrase] = buffer_df['URL'].to_list()  # добавить запись в словарь, где ключ – фраза, а значения – адреса

    final_df = pd.DataFrame(columns=columns)  # пустой финальный DataFrame
    group_num = 1  # начальное значение группы

    print('\nЭтап II/IV')  # вывод информации о начале второго этапа работы над файлом
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
                            continue  # и начать цикл заново

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
    reduced_df = df.loc[:, 'PHRASES':'GROUP']  # сокращение размеров DataFrame
    groups = list(sorted(set(reduced_df['GROUP'].to_list())))  # список всех групп
    length = len(groups)  # длина списка с группами, нужна для статус-бара
    group_phrase_dir = {}  # пустой словаря, где ключи – группы, а значениея – список фраз
    valid_groups = []  # список, где будут сохраняться группы без повторений

    print('Этап III/IV')  # вывести этап работы
    # цикл для заполнения group_phrase_dir
    for index, group in enumerate(groups):  # для каждой фразы и ее индекса в списке групп
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
        group_phrase_dir[group] = reduced_df.loc[df['GROUP'] == group]['PHRASES'].to_list()  # в group_phrase_dir

    print('\nЭтап IV/IV')  # вывести этап работы
    # цикл, определяющий группы, фразы в которых НЕ содержаться в других группах. Эти группы помещаются в valid_groups
    index = 0  # начальный индекс
    while True:  # бесконечный цикл
        length = len(groups)  # длина списка с группами, нужна для статус-бара
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп
        if index == length - 1:  # если индекс последней группы в списке групп то
            break  # завешить цикл

        group = groups[index]  # основная рабочая группа
        phrases = group_phrase_dir[group]  # фразы основной рабочей группы

        # цикл для перебора и сравнения фраз последующих групп с фразами основной группы
        for next_group in groups[index + 1:]:  # для каждой следующей группы
            phrases_next = group_phrase_dir[next_group]  # определить фразы этой группы
            intersection = list(set(phrases) & set(phrases_next))  # перечечение списков для основной и текущей группы
            if intersection == phrases:  # если пересечение фраз равно списку фраз для основной группы, то
                groups.remove(group)  # удалить из списка групп номер основной группы
                group = next_group  # и присвоить ему заначение текущей группы
            elif intersection == phrases_next:  # если пересечение фраз равно списку фраз для текущей группы, то
                groups.remove(next_group)  # удалить из списка групп номер текущей группы
            else:  # если перечечений нет, то
                continue  # продолжить цикл с новой текущей группой
        else:  # по завершению цикла проверить
            if group == groups[index]:  # если True, то эта группа уникальна и она не была удалена из списка групп
                valid_groups.append(group)  # добавить номер группы в valid_groups
                index += 1  # увеличить счетчик цикла и перезапустить цикл с новой основной группой
            else:  # если эта группа содержалась в другой группе, то она была удалена и под этим индексом будет
                # уже другая группа, значить индекс увеличивать  е нужно
                valid_groups.append(group)  # добавить номер группы в valid_groups

    return df[df['GROUP'].isin(valid_groups)]  # вернуть новый DataFrame, содержащий только уникальные группы


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
        final_df = group(phrases, df)  # групировка фраз и создание final_df

        file_dir = file[:-4]  # по сути, это имя файла без расширения
        if not os.path.exists(f'{result_dir}/{file_dir}'):  # если данная дирректория не существует,
            os.mkdir(f'{result_dir}/{file_dir}')  # то ее необходимо создать

        final_df = final_df.reset_index(drop=True)  # реиндексирование
        save_rest(df, final_df, phrases, result_dir, file_dir)  # сохранение несгрупированных фраз в отделный файл

        final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_grouped.csv', sep=';', index=False)  # запись в файл

        final_df = filtration(final_df) # вызов функции по удалению групп, содержащихся в других группах
        final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_filtered.csv', sep=';', index=False)  # запись в файл

    print('\nDONE!')
