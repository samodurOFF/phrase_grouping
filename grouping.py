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


def group(df):
    phrases = list(sorted(set(df['PHRASES'].to_list())))  # получить список фраз
    length = len(phrases)  # посчитать длину списка с фразами
    url_dict = {}  # создать пустой именованный массив, в котором ключ – фраза, а значения – адреса
    print('Этап I/IV')  # вывести этап работы
    for index, phrase in enumerate(phrases):  # для каждой фразы и ее индекса в списке групп
        print(f'\r{round(index / (length - 1) * 100, 2)}%', end='')  # вывести процент обработанных групп и записать
        buffer_df = df.loc[df['PHRASES'] == phrase]  # создать DataFrame с url
        url_dict[phrase] = buffer_df['URL'].to_list()  # добавить запись в словарь

    final_df = pd.DataFrame()  # пустой финальный DataFrame
    group_num = 1  # начальное значение группы

    print('\nЭтап II/IV')  # вывод информации о начале второго этапа работы над файлом
    for i in range(len(phrases)):  # для каждой фразы в списке фраз
        print(f'\r{round(i / (length - 1) * 100, 2)}%', end='')  # вывести процент отработанных фраз
        if i == length - 1:  # если индекс последней группы в списке групп то
            break  # завешить цикл

        main_phrase = phrases[i]  # определить значение начальной фразы
        main_urls = url_dict[main_phrase]  # получить список адресов начальной init_phrase
        intersection = main_urls  # и сделать этот список списком начального перечения
        min_count_main = 0  # минимальное количество главных страниц
        grouped_phrases = []  # список фраз имеющих одну группу

        for current_phrase in phrases[i + 1:]:  # для каждой фразы в списке фраз, не включающий предидущие фразы
            # print(target_phrase) # вывести фразу
            current_urls = url_dict[current_phrase]  # получить список адресов target_phrase
            new_intersection = list(set(intersection) & set(current_urls))  # найти пересечение адресов двух фраз
            if len(new_intersection) >= N:  # если количество пересечений больше или равно N
                grouped_phrases.append(current_phrase)  # то добавить фразу в список фраз одной группы
                intersection = new_intersection  # присвоить списку с пересечениями новое значение
            else:  # если количество перечечений меньше N, то
                continue  # начать цикл заново
        else:
            if len(grouped_phrases) == 0:  # если проход по фразе не встретил перечечений ни с одной другой фразой, то
                continue  # начать цикл заново перейдя к новой начальной фразе
            elif len(final_df):
                urls = final_df['URLS'].to_list()  # список всех адресов в final_df
                if intersection in urls:  # если список с пересекающимися адресами есть в списке с адресами из final_df
                    continue # то перезапустить цикл

            grouped_phrases.append(main_phrase)  # добавить начальную фразу
            main_page_count = count_main_urls(intersection)  # посчитать количество главных страниц
            final_dict = {
                'PHRASES': grouped_phrases,
                'GROUP': [group_num for phrase in grouped_phrases],
                'MAIN_PAGE_COUNT': [main_page_count for phrase in grouped_phrases],
                'URLS': [intersection for phrase in grouped_phrases]
            }
            # print(final_dict)
            if final_df.empty:  # если финальный DataFrame имеет тип None
                final_df = pd.DataFrame(final_dict)  # создать его структору с final_dict
            else:  # если финальный DataFrame не пустой, то
                final_df = final_df.append(pd.DataFrame(final_dict))  # добавить в него dataFrame с final_dict
            group_num += 1  # увеличить группу на 1

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
        final_df = group(df)  # групировка фраз и создание final_df

        file_dir = file[:-4]  # по сути, это имя файла без расширения
        if not os.path.exists(f'{result_dir}/{file_dir}'):  # если данная дирректория не существует,
            os.mkdir(f'{result_dir}/{file_dir}')  # то ее необходимо создать

        final_df = final_df.reset_index(drop=True)  # реиндексирование
        phrases = list(sorted(set(df['PHRASES'].to_list())))  # получить список фраз из начального dataFrame
        save_rest(df, final_df, phrases, result_dir, file_dir)  # сохранение несгрупированных фраз в отделный файл

        final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_grouped.csv', sep=';', index=False)  # запись в файл

        final_df = filtration(final_df) # вызов функции по удалению групп, содержащихся в других группах
        final_df.to_csv(f'{result_dir}/{file_dir}/{file_dir}_filtered.csv', sep=';', index=False)  # запись в файл

    print('\nDONE!')
