import os
import pandas as pd
from numpy import std, mean


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


def check_phrase(grouped_phrases, group_phrase_dict):
    """
    Функция проверяет содержаться ли все фразы одной группы, в другой

    :param grouped_phrases: фразы группы, которые проверяются на вхождение в другую ггруппу
    :param group_phrase_dict: словарь, где ключи – номера групп, а значения – фразы
    :return: возвращает True, если фразы содержаться в какой-нибудь группе
    """
    for phrases in group_phrase_dict.values():  # для каждого списка фраз из словаря
        if len(grouped_phrases) >= len(phrases):  # если длина проверяемого списка фраз больше текущего списка фраз, то
            continue  # начинаем цикл заново с новой текущей фразой
        elif set(grouped_phrases).issubset(phrases):  # если проверяемый список с фразами содержиться в текущем, то
            break  # прерываем цикл и возвращаем True
        else:  # если обу условия не верны, то перезапускаем цикл с новой текущей фразой
            continue  # перезапускаем цикл с новой текущей фразой
    else:  # если не было обнаружено ни одного прохода, то возвращаем
        return False  # возвращаем False
    return True  # если был break


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
                        l_i = s_i_j / r_i + l_i  # посчитать промежуточный коэффициент итогового ранжирования
            else:
                ratio_dict['GROUP'].append(group_number)  # добавление номера группы в финальный словарь
                ratio_dict['URL'].append(grouped_url)  # добавление адреса в финальный словарь
                ratio_dict['L_RATIO'].append(l_i / n)  # добавление коэффициента в финальный словарь
        else:
            return ratio_dict
    else:
        ratio_dict = {'GROUP': [], 'URL': [], 'C_V': [], 'V_R': [], 'C_V * V_R': []}  # финальный словарь
        for grouped_url in grouped_urls:  # для каждого сгруппированного адреса из списка сгруппированных адресов
            values_list = []  # создать список со значеними индекса этих адресов
            for phrase in grouped_phrases:  # для каждой сгруппированной фразы из списка сгруппированных адресов
                urls = main_dict[phrase]  # получить список адресов для конкретной фразы из исходного файла
                for index, url in enumerate(urls, start=1):  # для каждого индекса, адреса в списке исходных адресов
                    if grouped_url == url:  # если адрес из сгруппированного списка совпадает с адресом из исходника
                        values_list.append(index)  # добавить индекс этого адреса в список
            else:  # после цикла
                avr = mean(values_list)  # получить среднее
                c_v = std(values_list) / avr  # получить вариацию
                v_r = (max(values_list) - 1) / avr  # НЕ осцилляция
                ratio_dict['GROUP'].append(group_number)  # добавление номера группы в финальный словарь
                ratio_dict['URL'].append(grouped_url)  # добавление адреса в финальный словарь
                ratio_dict['C_V'].append(c_v)  # добавление коэффициента вариации в финальный словарь
                ratio_dict['V_R'].append(v_r)  # добавление коэффициента осцилляции в финальный словарь
                ratio_dict['C_V * V_R'].append(c_v * v_r)  # добавление произведения c_v * v_r
        else:
            return ratio_dict


def group(df):
    phrases = list(sorted(set(df['PHRASES'].to_list())))  # получить список фраз
    length = len(phrases)  # посчитать длину списка с фразами
    all_urls = {}  # пустой словарь, в котором ключ – фраза, а значения –  ВСЕ адреса для этой фразы
    ignored_urls = {}  # пустой словарь, в котором ключ – фраза, а значения – адреса, за исключением игнорируемых
    print('Этап I/II')  # вывести этап работы
    for index, phrase in enumerate(phrases):  # для каждой фразы и ее индекса в списке фраз
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
        buffer_df = df.loc[df['PHRASES'] == phrase]  # создать DataFrame по фразе
        url_list = buffer_df['URL'].to_list()  # список адресов для текущей фразы
        all_urls[phrase] = url_list.copy()  # добавить в словарь список всех адресов по фразе
        for domain in ignored_list:  # для каждого игнорируемого домена из списка игнорируемых доменов
            index = 0  # задать счетчик цикла равным 0
            while index != len(url_list):  # прекратить цикл как только индекс превысит значение длины списка с адресами
                url = url_list[index]  # проверяемый адрес
                if domain == url:  # если игнорируемый домен совпадает с адресом, то
                    url_list.pop(index)  # удалить этот адрес из списка адресов
                else:  # если не присутствует, то перейти к следующему адресу
                    index += 1  # и увеличить индекс на 1
        else:
            ignored_urls[phrase] = url_list  # добавить в словарь список адресов по фразе, кроме игнорируемых

    final_df = pd.DataFrame()  # пустой финальный DataFrame для сгруппированных данных
    ratio_df = pd.DataFrame()  # пустой финальный DataFrame для с данными коэффициентов
    group_num = 1  # начальное значение группы
    group_phrase_dict = {}  # пустой словаря, где ключи – группы, а значениея – список фраз

    print('\nЭтап II/II')  # вывод информации о начале второго этапа работы над файлом
    for i in range(len(phrases)):  # для каждой фразы в списке фраз
        print(f'\r{round(i / (length - 1) * 100, 2)}%', end='')  # вывести процент отработанных фраз
        if i == length - 1:  # если индекс последней группы в списке групп то
            break  # завершить цикл

        main_phrase = phrases[i]  # определить значение начальной фразы
        main_urls = ignored_urls[main_phrase]  # получить список адресов начальной init_phrase
        intersection = main_urls  # и сделать этот список списком начального перечения
        grouped_phrases = []  # список фраз, имеющих одну группу

        for current_phrase in phrases[i + 1:]:  # для каждой фразы в списке фраз, не включающий предидущие фразы
            # print(target_phrase) # вывести фразу
            current_urls = ignored_urls[current_phrase]  # получить список адресов target_phrase
            new_intersection = list(set(intersection) & set(current_urls))  # найти пересечение адресов двух фраз
            if len(new_intersection) >= N:  # если количество пересечений больше или равно N
                grouped_phrases.append(current_phrase)  # то добавить фразу в список фраз одной группы
                intersection = new_intersection  # присвоить списку с пересечениями новое значение
            else:  # если количество перечечений меньше N, то
                continue  # начать цикл заново с новой current_phrase
        else:
            grouped_phrases.insert(0, main_phrase)  # добавить начальную фразу
            if len(grouped_phrases) == 1:  # если проход по фразе не встретил перечечений ни с одной другой фразой, то
                continue  # начать цикл заново, перейдя к новой начальной фразе
            elif check_phrase(grouped_phrases, group_phrase_dict):  # если данный список с фразами содержиться в списке
                # фраз другой группы, то
                continue  # начать цикл заново, перейдя к новой начальной фразе
            else:  # если данный список с фразами НЕ содержиться в списке фраз другой группы, то
                ratio_dict = ratio(all_urls, grouped_phrases, intersection, group_num, type_ratio)  # коэффициенты
                ratio_df = ratio_df.append(pd.DataFrame(ratio_dict))  # добавить в итоговый DataFrame
                group_phrase_dict[group_num] = grouped_phrases  # дабавить список фраз к ключу с группой этой фразы
                main_page_count = count_main_urls(intersection)  # посчитать количество главных страниц
                # создать final_dict со следующей структурой
                final_dict = {
                    'PHRASES': grouped_phrases,
                    'GROUP': [group_num for phrase in grouped_phrases],
                    'MAIN_PAGE_COUNT': [main_page_count for phrase in grouped_phrases],
                    'URLS': [', '.join(intersection) for phrase in grouped_phrases]
                }
                # print(final_dict)
                if final_df.empty:  # если финальный DataFrame имеет тип None
                    final_df = pd.DataFrame(final_dict)  # создать его структору с final_dict
                else:  # если финальный DataFrame не пустой, то
                    final_df = final_df.append(pd.DataFrame(final_dict))  # добавить в него dataFrame с final_dict
                group_num += 1  # увеличить группу на 1

    return final_df, ratio_df


def filtration(df):
    reduced_df = df.loc[:, 'PHRASES':'GROUP']  # сокращение размеров DataFrame
    groups = list(sorted(set(reduced_df['GROUP'].to_list())))  # список всех групп
    length = len(groups)  # длина списка с группами, нужна для статус-бара
    group_phrase_dict = {}  # пустой словаря, где ключи – группы, а значениея – список фраз
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


def save_rest(init_df, final_df, phrases, result_dir, file_dir, N, ignoring):
    phrases_after = sorted(set(final_df['PHRASES'].to_list()))  # все сгруппированные фразы
    diff_list = list(set(phrases) - set(phrases_after))  # фразы, которые не были сгруппированы
    diff_df = init_df[init_df['PHRASES'].isin(diff_list)]  # сортирвоака по несгруппированным фразам
    diff_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_(N = {N}){ignoring}_rest.csv', sep=';', index=False)  # запись
    # print(f'\nФайл с несгрупированными фразами сохранен')


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
                           'Ответ: '))  # запрос у пользователя способа определения веса адреса
    ignoring = input('Игнорировать домены, указанные в ignored_domains.txt (y/n)?\n'
                     'Ответ: ')  # запрос у пользователя на игнорирование доменов
    if ignoring == 'y':  # если ответ пользователя разрешает игнорирование, то
        ignoring = '_ignored'  # присваеваем ignoring новое занчение, которое понадобиться для названия файла
        print('Игнорирование включено!')  # выводим, что игнорирование разрешено
    else:  # если пользоватлеь запрелит игнорирование
        ignored_list.clear()  # то список с игнорироемыми доменами очищается
        ignoring = ''  # присваеваем ignoring новое занчение, которое понадобиться для названия файла
        print('Игнорирование отключено!')  # # выводим, что игнорирование отключено

    if not os.path.exists(result_dir):  # если данная дирректория не существует,
        os.mkdir(result_dir)  # то ее необходимо создать

    print(f'Найдено файлов csv: {len(csv_files)} ')  # вывод информации о количестве файлов csv в консоль
    for file in csv_files:  # для каждого файла в списке
        print(f'\nРабота с файлом {file}')  # вывести имя файла
        df = pd.read_csv(file, sep=';')  # создать DataFrame
        final_df, ratio_df = group(df)  # групировка фраз и создание final_df

        file_dir = file[:-4]  # имя папки внутри папки result_dir
        file_name = f'{file_dir}_(N = {N}){ignoring}'  # имя сохраняемого файла
        if not os.path.exists(f'{result_dir}/{file_dir}'):  # если данная дирректория не существует,
            os.mkdir(f'{result_dir}/{file_dir}')  # то ее необходимо создать

        final_df = final_df.reset_index(drop=True)  # реиндексирование
        phrases = list(sorted(set(df['PHRASES'].to_list())))  # получить список фраз из начального dataFrame
        save_rest(df, final_df, phrases, result_dir, file_dir, N, ignoring)  # сохранение несгрупированных фраз в
        # отделный
        # файл

        final_df.to_csv(f'{result_dir}/{file_dir}/{file_name}_grouped.csv', sep=';', index=False)  # запись в
        # файл
        ratio_df.to_csv(f'{result_dir}/{file_dir}/{file_name}_coefficients.csv', sep=';', index=False)  # запись в файл

        # final_df = filtration(final_df)  # вызов функции по удалению групп, содержащихся в других группах
        # final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_filtered.csv', sep=';', index=False)  # запись в файл

    print('\nDONE!')
