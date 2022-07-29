import os
import time
from natsort import natsorted
import pandas as pd
from numpy import std, mean


def count_main_urls(iter):
    """
    Подсчет главных страниц

    :param iter: итерируемый объект (список или кортеж)
    :return:
    """
    count = 0  # входное значение
    for i in iter:  # перебор списка
        if i[-1] == '/' and i.count('/') <= 3:  # если url заканчивается на '/' и таких символов не более трех
            count += 1  # то увеличить count на 1
    return count


def check_phrase(group_phrases_dict, grouped_phrases):
    '''
    Функция проверяет содержаться ли все фразы одной группы, полностью в другой
    '''
    if grouped_phrases.difference(*group_phrases_dict.values()):
        return True

    return False


def ratio(main_dict, grouped_phrases, grouped_urls, group_number, type_ratio=1):
    if type_ratio == '1':  # если type_ratio равен 1 или не задан, то считаем l коэффициент
        length = len(grouped_phrases)  # количество фраз в группе
        ratio_list = []  # финальный список
        for grouped_url in grouped_urls:  # для каждого сгруппированного адреса из списка сгруппированных адресов
            l_i = 0  # начальное значение
            for phrase in grouped_phrases:
                urls = main_dict[phrase]  # список адресов для конкретной фразы из исходного файла
                r_i = len(urls)  # длина DataFrame по фразе
                for s_i_j, url in enumerate(urls, start=1):  # для каждого ранга, адреса в списке адресов из df_phrase
                    if grouped_url == url:  # если адрес из сгруппированного списка совпадает с адресом из исходника
                        l_i = s_i_j / r_i + l_i  # посчитать промежуточный коэффициент итогового ранжирования
            else:
                ratio_list.append({
                    'GROUP': group_number,  # добавление номера группы в финальный список
                    'URL': grouped_url,  # добавление адреса в финальный список
                    'L_RATIO': l_i / length  # добавление коэффициента в финальный список
                })

        else:
            return ratio_list
    elif type_ratio == '2':  # если type_ratio равен 2, то считаем коэффициент вариации и прочее
        ratio_list = []  # финальный список
        for grouped_url in grouped_urls:  # для каждого сгруппированного адреса из списка сгруппированных адресов
            values_list = []  # создать список со значениями индекса этих адресов
            for phrase in grouped_phrases:  # для каждой сгруппированной фразы из списка сгруппированных адресов
                urls = main_dict[phrase]  # получить список адресов для конкретной фразы из исходного файла
                for index, url in enumerate(urls, start=1):  # для каждого индекса, адреса в списке исходных адресов
                    if grouped_url == url:  # если адрес из сгруппированного списка совпадает с адресом из исходника
                        values_list.append(index)  # добавить индекс этого адреса в список
            else:  # после цикла
                avr = mean(values_list)  # получить среднее
                c_v = std(values_list) / avr  # получить вариацию
                v_r = (max(values_list) - 1) / avr  # НЕ осцилляция
                ratio_list.append({
                    'GROUP': group_number,  # добавление номера группы в финальный список
                    'URL': grouped_url,  # добавление адреса в финальный список
                    'C_V': c_v,  # добавление коэффициента вариации в финальный словарь
                    'V_R': v_r,  # добавление коэффициента осцилляции в финальный словарь
                    'C_V * V_R': c_v * v_r  # добавление произведения c_v * v_r
                })

        else:
            return ratio_list
    else:  # если type_ratio равен 3, то считаем все коэффициенты
        l_ratio = ratio(main_dict, grouped_phrases, grouped_urls, group_number, 1)  # используем эту функцию еще раз
        l_ratio = pd.DataFrame(l_ratio)['L_RATIO']  # создаем DF, чтобы быстро отсортировать
        ratio_list = ratio(main_dict, grouped_phrases, grouped_urls, group_number, 2)  # используем эту функцию еще раз
        ratio_df = pd.DataFrame(ratio_list)  # создаем DF, чтобы быстро отсортировать
        ratio_df['L_RATIO'] = l_ratio  # добавляем новый столбец
        final_df = ratio_df.sort_values('L_RATIO', ascending=True)  # сортируем
        return final_df.to_dict('records')  # преобразуем в список словарей


def get_group_name(grouped_phrases):
    """
    Функция для генерации имени для группы

    :param grouped_phrases:
    :return:
    """

    # список всех уникальных слов списка фраз группы
    all_words = sorted({word for phrase in grouped_phrases for word in phrase.split()}, reverse=True)
    return ' '.join([str(i) for i in all_words])  # объединение ключей в имя группы


def group(N, phrase_urls_dict, type_ratio):
    phrases = tuple(phrase_urls_dict.keys())
    length = len(phrases)  # посчитать длину списка с фразами

    final_list = []  # пустой финальный список для сгруппированных данных
    ratio_list = []  # пустой финальный список с данными коэффициентов
    group_phrases_dict = {}  # пустой словарь, где ключи – группы, а значение – список фраз
    group_num = 1  # начальное значение группы
    print('\nЭтап I/II')  # вывод информации об этапе

    for item, phrase in enumerate(phrases):  # для каждой фразы в списке фраз
        print(f'\r{round((item + 1) / length * 100, 2)}%', end='')  # вывести процент отработанных фраз
        intersection = phrase_urls_dict[phrase]  # список начального пересечения
        grouped_phrases = []  # список фраз, имеющих одну группу
        for current_phrase in phrases[item + 1:]:  # для каждой фразы в списке фраз, не включающий предыдущие фразы
            current_urls = phrase_urls_dict[current_phrase]  # получить список адресов
            new_intersection = intersection & current_urls  # найти пересечение адресов двух фраз
            if len(new_intersection) >= N:  # если количество пересечений больше или равно N
                grouped_phrases.append(current_phrase)  # то добавить фразу в список фраз одной группы
                intersection = new_intersection  # присвоить списку с пересечениями новое значение

        grouped_phrases.append(phrase)  # добавить начальную фразу
        grouped_phrases = set(grouped_phrases)
        if len(grouped_phrases) > 1:  # если проход по фразе нашел пересечение хотя бы с одной фразой
            if not grouped_phrases.difference(*group_phrases_dict.values()):
                continue
            # если данный список с фразами не содержится в списке фраз другой группы, то выполнить код ниже

            ratio_frame = ratio(phrase_urls_dict, grouped_phrases, intersection, group_num, type_ratio)  # коэффициенты
            ratio_list.extend(ratio_frame)  # добавить в итоговый ratio_list
            group_phrases_dict[group_num] = grouped_phrases  # добавить в словарь группа: список фраз
            group_name = get_group_name(grouped_phrases)
            main_page_count = count_main_urls(intersection)  # посчитать количество главных страниц
            # добавить в final_list список со словарями следующей структуры
            final_list.extend([{
                'PHRASES': phrase,
                'GROUP': str(group_num),
                'GROUP_NAME': group_name,
                'MAIN_PAGE_COUNT': main_page_count,
                'URLS': ', '.join(intersection)
            } for phrase in grouped_phrases])
            group_num += 1  # увеличить группу на 1

            # break

    return final_list, pd.DataFrame(ratio_list), group_phrases_dict


def two_stage_group(group_df, ratio_df, N, phrase_urls_dict, type_ratio):
    result_list = []  # пустой финальный список для сгруппированных данных
    ratio_list = []  # пустой финальный список с данными коэффициентов

    group_phrases_dict_main = {group: frame['PHRASES'] for group, frame in
                               natsorted(group_df.groupby('GROUP'))}  # {group: [phrase1, ...]}

    length = len(group_phrases_dict_main)  # длина словаря со сгруппированными фразами
    print('\nЭтап I/II')  # вывод информации об этапе

    for i, items in enumerate(group_phrases_dict_main.items()):
        group, phrases = items  # номер группы и ее фразы
        print(f'\r{round((i + 1 )/ (length) * 100, 2)}%', end='')  # вывести процент отработанных фраз

        result_buffer = []
        ratio_buffer = []
        group_num = 1  # начальное значение группы
        group_phrases_dict = {}  # пустой словарь, где ключи – группы, а значение – список фраз
        for item, phrase in enumerate(phrases):  # для каждой фразы в списке фраз
            intersection = phrase_urls_dict[phrase]  # список начального пересечения
            grouped_phrases = []  # список фраз, имеющих одну группу

            for current_phrase in phrases[item + 1:]:  # для каждой фразы в списке фраз, не включающий предыдущие фразы
                current_urls = phrase_urls_dict[current_phrase]  # получить список адресов
                new_intersection = intersection & current_urls  # найти пересечение адресов двух фраз
                if len(new_intersection) >= N:  # если количество пересечений больше или равно N
                    grouped_phrases.append(current_phrase)  # то добавить фразу в список фраз одной группы
                    intersection = new_intersection  # присвоить списку с пересечениями новое значение

            grouped_phrases.append(phrase)  # добавить начальную фразу
            grouped_phrases = set(grouped_phrases)  # сделать множеством

            if not grouped_phrases.difference(*group_phrases_dict.values()):
                continue
                # если данный список с фразами не содержится в списке фраз другой группы, то выполнить код ниже

            new_group_number = group if len(phrases) == len(grouped_phrases) else f'{group}_{group_num}'
            ratio_frame = ratio(phrase_urls_dict, grouped_phrases, intersection, new_group_number,
                                type_ratio)  # коэффициенты
            ratio_buffer.extend(ratio_frame)  # добавить в итоговый ratio_list
            group_phrases_dict[new_group_number] = grouped_phrases  # добавить в словарь группа: список фраз
            group_name = get_group_name(grouped_phrases)
            main_page_count = count_main_urls(intersection)  # посчитать количество главных страниц
            # добавить в buffer список со словарями следующей структуры
            result_buffer.extend([{
                'PHRASES': phrase,
                'GROUP': new_group_number,
                'GROUP_NAME': group_name,
                'MAIN_PAGE_COUNT': main_page_count,
                'URLS': ', '.join(intersection)
            } for phrase in grouped_phrases])

            if len(phrases) == len(grouped_phrases):
                break

            group_num += 1  # увеличить группу на 1

        if not result_buffer or len(result_buffer) == len(phrases):
            old_result = group_df[group_df['GROUP'] == group]
            name = set(old_result['GROUP_NAME']).pop().split(' [')[0]
            old_result = pd.DataFrame(old_result)
            old_result['GROUP_NAME'] = name
            old_result = old_result.to_dict(orient='records')  # старый список для группы
            result_list.extend(old_result)  # добавить без изменений

            old_ratios = ratio_df[ratio_df['GROUP'] == group].to_dict(orient='records')  # старый список коэффициентов
            ratio_list.extend(old_ratios)  # добавить без изменений

        else:
            result_list.extend(result_buffer)
            ratio_list.extend(ratio_buffer)

    return result_list, pd.DataFrame(ratio_list)


def save_rest(init_df, final_df, phrases, result_dir, file_dir, N, ignoring):
    phrases_after = sorted(set(final_df['PHRASES'].to_list()))  # все сгруппированные фразы
    diff_list = list(set(phrases) - set(phrases_after))  # фразы, которые не были сгруппированы
    diff_df = init_df[init_df['PHRASES'].isin(diff_list)]  # сортировка по несгруппированным фразам
    diff_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_(N = {N}){ignoring}_rest.csv', sep=';', index=False)  # запись
    # print(f'\nФайл с несгруппированными фразами сохранен')


def unique_search(df):
    '''
    Функция для поиска уникальных и неуникальных фраз

    :param df: сгруппированный DataFrame с фразами.
    :return:
    '''
    print('\nЭтап II/II')  # вывод информации об этапе
    length = len(df)  # длина массива
    current_group = None  # текущая группа
    new_name = ''  # новое имя группы
    list_of_dict = df.to_dict(orient='records')  # список словарей
    phrase_groups_dict = {phrase: frame['GROUP'] for phrase, frame in
                          natsorted(df.groupby('PHRASES'))}  # {phrase: [group1, ...]}
    group_phrases_dict = {group: set(frame['PHRASES']) for group, frame in
                          natsorted(df.groupby('GROUP'))}  # {group: [phrase1, ...]}
    group_name_dict = {group: tuple(frame['GROUP_NAME'])[0] for group, frame in
                       natsorted(df.groupby('GROUP'))}  # {group: [group_name1, ...]}
    for index, data in enumerate(list_of_dict):  # index - индекс словаря   , data – сам словарь
        print(f'\r{round((index + 1) / (length) * 100, 2)}%', end='')  # вывести процент отработанных фраз
        group = data['GROUP']
        group_name = group_name_dict[group]

        if group != current_group:
            current_group = group  # значение текущей группы
            uniq_phrases = set()  # уникальные фразы, те, что есть только в данной группе
            groups_with_non_uniq_phrases = []  # список с группами в которых находится неуникальная фраза
            phrases_by_group = group_phrases_dict[current_group]  # все фразы для текущей группы
            for phrase in phrases_by_group:  # для каждой фразы текущей группы
                groups_by_phrase = phrase_groups_dict[phrase]  # список групп, в которых присутствует эта фраза
                if len(groups_by_phrase) == 1:  # если True это значит, что фраза уникальна
                    uniq_phrases.add(phrase)  # добавляем фразу в набор уникальных фраз данной группы
                else:  # если False это значит, что фраза НЕ уникальна
                    groups_with_non_uniq_phrases.extend(groups_by_phrase)  # добавляем группы, в которых она встречается

            if not uniq_phrases:  # если uniq_phrases пуст, то это значит все фразы неуникальны
                groups_with_non_uniq_phrases = list(map(str, sorted(set(groups_with_non_uniq_phrases))))
                new_name = f"{group_name} [all phrases in - {', '.join(groups_with_non_uniq_phrases)}]"

            else:  # если False, значит все фразы уникальны
                new_name = f"{group_name} [{', '.join(uniq_phrases)}]"

            data['GROUP_NAME'] = new_name
        else:
            data['GROUP_NAME'] = new_name

    return pd.DataFrame(list_of_dict)


if __name__ == '__main__':
    dir = os.getcwd()  # вернуть текущую папку
    csv_files = [file for file in os.listdir(dir) if file.endswith(".csv")]  # список всех .csv файлов
    with open('ignored_domains.txt', 'r', encoding='utf-8') as file:
        ignored_list = file.read().splitlines()  # список игнорируемых доменов

    result_dir = 'RESULT'  # папка для сохранения результатов
    while True:
        # запрос у пользователя количества пересечений для поиска
        N = input('Укажите количество пересечений N: ')
        if N and N.isdigit():
            N = int(N)
            break

    while True:
        type_ratio = input('Укажите способ определения значимости адреса:\n'
                           '1 – через коэффициент итогового ранжирования (быстро),\n'
                           '2 – через коэффициенты вариации (медленно).\n'
                           '3 – оба.\n'
                           'Ответ: ')  # запрос у пользователя способа определения веса адреса
        if type_ratio in ('1', '2', '3'):
            break

    while True:
        ignoring = input('Игнорировать домены, указанные в ignored_domains.txt (y/n)?\n'
                         'Ответ: ')  # запрос у пользователя на игнорирование доменов
        if ignoring in ('y', 'n'):
            break

    ignoring_type = None
    if ignoring == 'y':  # если ответ пользователя разрешает игнорирование, то
        while True:
            ignoring_type = input('Выберете тип игнорирования:\n'
                                  '1 – по точному совпадению,\n'
                                  '2 – по вложенности.\n'
                                  'Ответ: ')  # запрос у пользователя то, как игнорировать домены
            if ignoring_type in ('1', '2'):
                break

        ignoring = '_ignored'  # присваиваем ignoring новое значение, которое понадобиться для названия файла
        print('Игнорирование включено!')  # выводим, что игнорирование разрешено
    else:  # если пользователь запретит игнорирование
        ignored_list.clear()  # то список с игнорируемыми доменами очищается
        ignoring = ''  # присваиваем ignoring новое значение, которое понадобиться для названия файла
        print('Игнорирование отключено!')  # # выводим, что игнорирование отключено

    if not os.path.exists(result_dir):  # если данная директория не существует,
        os.mkdir(result_dir)  # то ее необходимо создать

    print(f'Найдено файлов csv: {len(csv_files)} ')  # вывод информации о количестве файлов csv в консоль
    for file in csv_files:  # для каждого файла в списке
        print(f'\nРабота с файлом {file}')  # вывести имя файла
        df = pd.read_csv(file, sep=';')  # создать DataFrame
        phrases = sorted(set(df['PHRASES']))  # получить список фраз из начального dataFrame
        phrase_urls_dict = {phrase: frame['URL'] for phrase, frame in
                            natsorted(df.groupby('PHRASES'))}  # словарь: {фраза: [адрес1, адрес2, ...]}

        for index, phrase in enumerate(phrases):  # для каждой фразы и ее индекса в списке фраз
            url_list = phrase_urls_dict[phrase]
            if ignoring_type == '1':  # если тип игнорирования выставлен на совпадения или не задан, то
                # добавить в словарь список адресов по фразе, кроме игнорируемых
                phrase_urls_dict[phrase] = set(url for domain in ignored_list for url in url_list if url == domain)
            elif ignoring_type == '2':  # если тип игнорирования выставлен на вхождение, то
                # добавить в словарь список адресов по фразе, кроме игнорируемых
                phrase_urls_dict[phrase] = set(url for domain in ignored_list for url in url_list if url in domain)
            else:  # если игнорирование не задано, то
                phrase_urls_dict[phrase] = set(url_list)

        grouped, ratios, group_phrases_dict = group(
            N,
            phrase_urls_dict,
            type_ratio,
        )  # группировка фраз и создание final_df

        file_dir = file[:-4]  # имя папки внутри папки result_dir
        file_name = f'{file_dir}{ignoring}_(N = {N})'  # имя сохраняемого файла
        if not os.path.exists(f'{result_dir}/{file_dir}'):  # если данная директория не существует,
            os.mkdir(f'{result_dir}/{file_dir}')  # то ее необходимо создать

        grouped = pd.DataFrame(grouped).reset_index(drop=True)  # перевод в DataFrame и реиндексирование
        save_rest(df, grouped, phrases, result_dir, file_dir, N, ignoring)  # сохранение несгруппированных фраз в
        # отдельный файл

        ratios.to_csv(f'{result_dir}/{file_dir}/{file_name}_coefficients.csv', sep=';', index=False)  # запись в файл
        grouped = unique_search(grouped)
        grouped.to_csv(f'{result_dir}/{file_dir}/{file_name}_grouped.csv', sep=';', index=False)  # запись в файл

        while True:
            # запрос у пользователя количества пересечений для поиска
            N2 = input(
                '\nУкажите количество пересечений N для следующей ступени группировки или нажмите "enter" для пропуска: ')
            if N2.isdigit():
                N2 = int(N2)
            elif N2 == '':
                break

            grouped, ratios, = two_stage_group(grouped, ratios, N2, phrase_urls_dict, type_ratio)

            grouped = pd.DataFrame(grouped).reset_index(drop=True)  # перевод в DataFrame и реиндексирование

            file_name += f'_(N = {N2})'
            ratios.to_csv(f'{result_dir}/{file_dir}/{file_name}_coefficients.csv', sep=';',
                          index=False)  # запись в файл
            grouped = unique_search(grouped)
            grouped.to_csv(f'{result_dir}/{file_dir}/{file_name}_grouped.csv', sep=';',
                           index=False)  # запись в файл

    print('\nDONE!')
