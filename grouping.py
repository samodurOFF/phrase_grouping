import os

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


def check_phrase(grouped_phrases, group_phrase_dict):
    """
    Функция проверяет содержаться ли все фразы одной группы, в другой

    :param grouped_phrases: фразы группы, которые проверяются на вхождение в другую группу,
    :param group_phrase_dict: словарь, где ключи – номера групп, а значения – фразы,
    :return: возвращает True, если фразы содержаться в какой-нибудь группе
    """
    for phrases in group_phrase_dict.values():  # для каждого списка фраз из словаря
        if set(grouped_phrases).issubset(phrases):  # если проверяемый список с фразами содержится в текущем, то
            break  # прерываем цикл и возвращаем True
    else:  # если не было обнаружено ни одного вложения, то
        return False  # возвращаем False
    return True  # если был break


def ratio(main_dict, grouped_phrases, grouped_urls, group_number, type_ratio=1):
    if type_ratio == 1:  # если type_ratio равен 1 или не задан, то считаем l коэффициент
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
    elif type_ratio == 2:  # если type_ratio равен 2, то считаем коэффициент вариации и прочее
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
    all_words = []  # список, куда будут помещаться все слова всех фраз группы
    for phrase in grouped_phrases:  # для каждой фразы из группы
        all_words.extend(phrase.split())  # разделить на слова и поместить в список

    all_words_uniq = sorted(set(all_words))  # уникальные слова всех фраз, отсортированные по возрастанию

    # словарь, где ключи – слова, а значения – частоты их употребления во всех словах группы
    freq_dict = {word: all_words.count(word) for word in all_words_uniq}

    sorted_words = sorted(freq_dict, reverse=True)  # список слов, значения которых упорядочены по убыванию

    group_name = ' '.join([str(i) for i in sorted_words])  # объединение ключей в имя группы
    return group_name


def group(df, ignored_list, ignoring_type=None):
    phrases = tuple(set(df['PHRASES'].to_list()))  # получить список фраз
    length = len(phrases)  # посчитать длину списка с фразами
    ignored_urls = {i[0]: list(i[1]['URL']) for i in df.groupby('PHRASES')}  # словарь: {фраза: [адрес1, адрес2, ...]}
    print('Этап I/III')  # вывести этап работы
    for index, phrase in enumerate(phrases):  # для каждой фразы и ее индекса в списке фраз
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
        url_list = ignored_urls[phrase]  # список адресов для текущей фразы

        if ignoring_type == '1':  # если тип игнорирования выставлен на совпадения или не задан, то
            # добавить в словарь список адресов по фразе, кроме игнорируемых
            ignored_urls[phrase] = [url for domain in ignored_list for url in url_list if url == domain]
        elif ignoring_type == '2':  # если тип игнорирования выставлен на вхождение, то
            # добавить в словарь список адресов по фразе, кроме игнорируемых
            ignored_urls[phrase] = [url for domain in ignored_list for url in url_list if url in domain]
        else:
            ignored_urls[phrase] = url_list

    final_list = []  # пустой финальный список для сгруппированных данных
    ratio_list = []  # пустой финальный список с данными коэффициентов
    group_num = 1  # начальное значение группы
    group_phrase_dict = {}  # пустой словарь, где ключи – группы, а значение – список фраз

    print('\nЭтап II/III')  # вывод информации о начале второго этапа работы над файлом
    for i in range(length - 1):  # для каждой фразы в списке фраз
        print(f'\r{round(i / (length - 2) * 100, 2)}%', end='')  # вывести процент отработанных фраз
        main_phrase = phrases[i]  # определить значение начальной фразы
        main_urls = ignored_urls[main_phrase]  # получить список адресов начальной main_phrase
        intersection = main_urls  # и сделать этот список списком начального пересечения
        grouped_phrases = []  # список фраз, имеющих одну группу

        for current_phrase in phrases[i + 1:]:  # для каждой фразы в списке фраз, не включающий предыдущие фразы
            current_urls = ignored_urls[current_phrase]  # получить список адресов
            new_intersection = list(set(intersection) & set(current_urls))  # найти пересечение адресов двух фраз
            if len(new_intersection) >= N:  # если количество пересечений больше или равно N
                grouped_phrases.append(current_phrase)  # то добавить фразу в список фраз одной группы
                intersection = new_intersection  # присвоить списку с пересечениями новое значение

        else:
            grouped_phrases.append(main_phrase)  # добавить начальную фразу
            if len(grouped_phrases) > 1 and not check_phrase(grouped_phrases, group_phrase_dict):
                # если проход по фразе нашел пересечение хотя бы с одной фразой и данный список с фразами не содержится
                # в списке фраз другой группы, то выполнить код ниже

                ratio_frame = ratio(ignored_urls, grouped_phrases, intersection, group_num, type_ratio)  # коэффициенты
                ratio_list.extend(ratio_frame)  # добавить в итоговый ratio_list
                group_phrase_dict[group_num] = grouped_phrases  # добавить список фраз к ключу с группой этой фразы
                group_name = get_group_name(grouped_phrases)
                main_page_count = count_main_urls(intersection)  # посчитать количество главных страниц
                # добавить в final_list список со словарями следующей структуры
                final_list.extend([{
                    'PHRASES': phrase,
                    'GROUP': group_num,
                    'GROUP_NAME': group_name,
                    'MAIN_PAGE_COUNT': main_page_count,
                    'URLS': ', '.join(intersection)
                } for phrase in grouped_phrases])
                group_num += 1  # увеличить группу на 1

    return pd.DataFrame(final_list), pd.DataFrame(ratio_list)


def filtration(df):
    reduced_df = df.loc[:, 'PHRASES':'GROUP']  # сокращение размеров DataFrame
    groups = list(sorted(set(reduced_df['GROUP'].to_list())))  # список всех групп
    length = len(groups)  # длина списка с группами, нужна для статус-бара
    group_phrase_dict = {}  # пустой словаря, где ключи – группы, а значения – список фраз
    valid_groups = []  # список, где будут сохраняться группы без повторений

    print('Этап III/IV')  # вывести этап работы
    # цикл для заполнения group_phrase_dir
    for index, group in enumerate(groups):  # для каждой группы и ее индекса в списке групп
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
        group_phrase_dict[group] = reduced_df.loc[reduced_df['GROUP'] == group]['PHRASES'].to_list()  # в
        # group_phrase_dir

    print('\nЭтап IV/IV')  # вывести этап работы
    # цикл, определяющий группы, фразы в которых НЕ содержаться в других группах. Эти группы помещаются в valid_groups
    index = 0  # начальный индекс
    while True:  # бесконечный цикл
        length = len(groups)  # длина списка с группами, нужна для статус-бара
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп
        if index == length - 1:  # если индекс последней группы в списке групп то
            break  # завершить цикл

        group = groups[index]  # основная рабочая группа
        phrases = group_phrase_dict[group]  # фразы основной рабочей группы

        # цикл для перебора и сравнения фраз последующих групп с фразами основной группы
        for next_group in groups[index + 1:]:  # для каждой следующей группы
            phrases_next = group_phrase_dict[next_group]  # определить фразы этой группы
            intersection = list(set(phrases) & set(phrases_next))  # пересечение списков для основной и текущей группы
            if intersection == phrases:  # если пересечение фраз равно списку фраз для основной группы, то
                groups.remove(group)  # удалить из списка групп номер основной группы
                group = next_group  # и присвоить ему значение текущей группы
            elif intersection == phrases_next:  # если пересечение фраз равно списку фраз для текущей группы, то
                groups.remove(next_group)  # удалить из списка групп номер текущей группы
            else:  # если пересечений нет, то
                continue  # продолжить цикл с новой текущей группой
        else:  # по завершению цикла проверить
            if group == groups[index]:  # если True, то эта группа уникальна и она не была удалена из списка групп
                valid_groups.append(group)  # добавить номер группы в valid_groups
                index += 1  # увеличить счетчик цикла и перезапустить цикл с новой основной группой
            else:  # если эта группа содержалась в другой группе, то она была удалена и под этим индексом будет
                # уже другая группа, значить индекс увеличивать  е нужно
                valid_groups.append(group)  # добавить номер группы в valid_groups

    return df[df['GROUP'].isin(valid_groups)]  # вернуть новый DataFrame, содержащий только уникальные группы


def save_rest(init_df, final_df, phrases, result_dir, file_dir, N, ignoring):
    phrases_after = sorted(set(final_df['PHRASES'].to_list()))  # все сгруппированные фразы
    diff_list = list(set(phrases) - set(phrases_after))  # фразы, которые не были сгруппированы
    diff_df = init_df[init_df['PHRASES'].isin(diff_list)]  # сортировка по несгруппированным фразам
    diff_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_(N = {N}){ignoring}_rest.csv', sep=';', index=False)  # запись
    # print(f'\nФайл с несгруппированными фразами сохранен')


def unique_search(df):
    length = len(df)
    print('\nЭтап III/III')  # вывод информации об этапе
    current_group = None
    for index in range(len(df)):
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
        group = df.at[index, 'GROUP']
        if group == current_group:
            df.at[index, 'GROUP_NAME'] = df.at[index - 1, 'GROUP_NAME']
            continue
        else:
            current_group = group
            uniq_phrases = []  # список с уникальными фразами, теми, которые есть только в данной группе
            groups_with_non_uniq_phrases = []  # список с группами в которых находиться не уникальная фраза
            phrases_by_group = df[df['GROUP'] == group]['PHRASES'].to_list()  # все фразы для одной группы
            for phrase in phrases_by_group:
                groups_by_phrase = df[df['PHRASES'] == phrase]['GROUP'].to_list()  # все группы для одной фразы
                if len(groups_by_phrase) == 1:  # если True это значит, что фраза уникальна
                    uniq_phrases.append(phrase)  # добавляем фразу в список уникальных фраз
                else:  # если True это значит, что фраза НЕ уникальна
                    groups_with_non_uniq_phrases.extend(groups_by_phrase)  # добавляем группы, в которых она встречается
            else:
                if len(uniq_phrases) == 0:  # если True, значит все фразы не уникальны
                    groups_with_non_uniq_phrases = [str(num) for num in sorted(set(groups_with_non_uniq_phrases))]
                    df.at[
                        index, 'GROUP_NAME'] = f"{df.at[index, 'GROUP_NAME']} [all phrases in - {', '.join(groups_with_non_uniq_phrases)}]"
                else:  # если False, значит все фразы уникальны
                    df.at[index, 'GROUP_NAME'] = f"{df.at[index, 'GROUP_NAME']} [{', '.join(uniq_phrases)}]"
    else:
        return df


def second_group(df, N):
    pass


if __name__ == '__main__':
    dir = os.getcwd()  # вернуть текущую папку
    csv_files = [file for file in os.listdir(dir) if file.endswith(".csv")]  # список всех .csv файлов
    with open('ignored_domains.txt', 'r', encoding='utf-8') as file:
        ignored_list = file.read().splitlines()  # список игнорируемых доменов

    result_dir = 'RESULT'  # папка для сохранения результатов
    N = int(input('Укажите количество пересечений N: '))  # запрос у пользователя количества пересечений для поиска
    type_ratio = int(input('Укажите способ определения значимости адреса:\n'
                           '1 – через коэффициент итогового ранжирования (быстро),\n'
                           '2 – через коэффициенты вариации (медленно).\n'
                           '3 – оба.\n'
                           'Ответ: '))  # запрос у пользователя способа определения веса адреса
    ignoring = input('Игнорировать домены, указанные в ignored_domains.txt (y/n)?\n'
                     'Ответ: ')  # запрос у пользователя на игнорирование доменов
    ignoring_type = None
    if ignoring == 'y':  # если ответ пользователя разрешает игнорирование, то
        ignoring_type = input('Выберете тип игнорирования:\n'
                              '1 – по точному совпадению,\n'
                              '2 – по вложенности.\n'
                              'Ответ: ')  # запрос у пользователя то, как игнорировать домены
        ignoring = '_ignored'  # присваиваем ignoring новое значение, которое понадобиться для названия файла
        print('Игнорирование включено!')  # выводим, что игнорирование разрешено
    else:  # если пользователь запретит игнорирование
        ignored_list.clear()  # то список с игнорируемыми доменами очищается
        ignoring = ''  # присваиваем ignoring новое значение, которое понадобиться для названия файла
        print('Игнорирование отключено!')  # выводим, что игнорирование отключено

    if not os.path.exists(result_dir):  # если данная директория не существует,
        os.mkdir(result_dir)  # то ее необходимо создать

    print(f'Найдено файлов csv: {len(csv_files)} ')  # вывод информации о количестве файлов csv в консоль
    for file in csv_files:  # для каждого файла в списке
        print(f'\nРабота с файлом {file}')  # вывести имя файла
        df = pd.read_csv(file, sep=';')  # создать DataFrame
        final_df, ratio_df = group(df, ignored_list, ignoring_type)  # группировка фраз и создание final_df

        file_dir = file[:-4]  # имя папки внутри папки result_dir
        file_name = f'{file_dir}_(N = {N}){ignoring}'  # имя сохраняемого файла
        if not os.path.exists(f'{result_dir}/{file_dir}'):  # если данная директория не существует,
            os.mkdir(f'{result_dir}/{file_dir}')  # то ее необходимо создать

        final_df = final_df.reset_index(drop=True)  # реиндексирование
        phrases = list(sorted(set(df['PHRASES'].to_list())))  # получить список фраз из начального dataFrame
        save_rest(df, final_df, phrases, result_dir, file_dir, N, ignoring)  # сохранение несгруппированных фраз в
        # отдельный файл

        ratio_df.to_csv(f'{result_dir}/{file_dir}/{file_name}_coefficients.csv', sep=';', index=False)  # запись в файл

        final_df = unique_search(final_df)

        final_df.to_csv(f'{result_dir}/{file_dir}/{file_name}_grouped.csv', sep=';', index=False)  # запись в файл

        # final_df = filtration(final_df)  # вызов функции по удалению групп, содержащихся в других группах
        # final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_filtered.csv', sep=';', index=False)  # запись в файл

        # if input('Провести группировку по URL (y/n)?\nОтвет: ') == 'y':
        #     N2 = int(input('Укажите количество пересечений N: '))  # количество пересечений
        #     df = pd.read_csv(f'{result_dir}/{file_dir}/{file_name}_grouped.csv', sep=';', )

    print('\nDONE!')
