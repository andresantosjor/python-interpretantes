import os.path
import sys
import sqlite3
from terminaltables import AsciiTable
import requests
from datetime import datetime
from dotenv import dotenv_values
from plotnine import *
from openpyxl import Workbook

secret = dotenv_values('config.env')

def create_database(cursor):
    cursor.execute('''CREATE TABLE threads
             (id integer,
              title text,
              dislikes integer,
              likes integer,
              created_at text,
              posts integer,
              url text 
             )''')
    
    cursor.execute('''CREATE TABLE posts
             (id integer,
              thread integer,
              parent integer,
              created_at text, 
              message text,
              media text,
              likes integer,
              dislikes integer,
              author_user text,
              author_name text,
              author_bio text,
              author_url text,
              interpretant text
             )''')

def check_and_connect(db_name):
    db_exists = False
    if os.path.isfile(db_name):
        db_exists = True

    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    if not db_exists:
        create_database(cursor)

    return (connection, cursor)

def opt_download(connection, cursor):
    print('Opções: \n\t0 - Download thread\n\t1 - Atualizar thread')
    user = input('> ')
    while(True):
        if (user == '0'):
            thread_id = input('Número da thread: ')
            parameters = {
                "api_key": secret['API_KEY'],
                "thread": thread_id,
            }
            print('Buscando thread {}...'.format(thread_id))
            response = requests.get("https://disqus.com/api/3.0/threads/list.json", parameters)

            if (response.status_code == 200):
                thread_info = response.json()['response'][0]
    
                thread = (
                    thread_info['id'], 
                    thread_info['title'], 
                    thread_info['dislikes'], 
                    thread_info['likes'],
                    thread_info['createdAt'],
                    thread_info['posts'],
                    thread_info['link']
                )
    
                cursor.execute('INSERT INTO threads VALUES(?,?,?,?,?,?,?)', thread)
                connection.commit()
                print('Thread atualizada.')

                print('Buscando posts...')
                fetch_posts(connection, cursor, thread_id)
                connection.commit()
                print('Posts atualizados.')
            else:
                print('Thread não encontrada.')

            break

        if (user == '1'):
            print('Hi')
            break
    return

def opt_thread(connection, cursor):
    rows = cursor.execute('SELECT ROW_NUMBER () OVER (ORDER BY created_at DESC ) RowNum, id, title, posts, created_at FROM threads').fetchall()
    threads_list = [['Num', 'ID', 'Título', 'Posts', 'Data']] + [list(elem) for elem in rows]
    table = AsciiTable(threads_list)

    print(table.table)
    return threads_list

def opt_interpretants(cursor):
    pass

def opt_export(cursor):
    pass

def generate_graph(df):
    plot = (
        ggplot(data=df)
        + geom_point(aes(x="created_date", y="final_time", color="interpretant"))
        + scale_x_date(name="", date_labels="%d-%b")
        + scale_y_datetime(date_labels="%H:%M")
        + labs(y="Hora Publicação", color="Interpretante")
        # + ggtitle("One Piece 990 - Comentários")
        + theme_minimal()
        + theme(plot_background=element_rect(color="white")) # Avoid transparent background.
    )

    print('Gráfico criado.')
    name = input('Dê nome ao gráfico: ')

    ggsave(plot, "{}.png".format(name), width=6.8, height=4.8, units="in", dpi=300)

    print('Gráfico criado.')

def opt_graph():
    import locale
    import pandas as pd

    locale.setlocale(locale.LC_TIME, "pt_BR")

    csv = input('Localização do CSV: ')
    df = pd.read_csv(csv, delimiter=";")
    
    # Arrumar data
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['created_date'] = df['created_at'].dt.date
    df['created_time'] = df['created_at'].dt.time
    df['final_time'] = pd.to_datetime(df['created_time'], format='%H:%M:%S')

    # Arrumar interpretante
    df['interpretant'].replace({'em': 'Emocional', 'en': 'Energético', 'lg': 'Lógico'}, inplace=True)

    generate = input('Deseja gerar o gráfico? (s/n) ')
    if generate in ['s', 'S', 'y', 'Y']:
        generate_graph(df)
    
    show_data = input('Deseja visualizar os dados? (s/n) ')
    if show_data in ['s', 'S', 'y', 'Y']:
        print(df[["created_date", "interpretant"]].groupby(['created_date'])['interpretant'].count())
        print(df[["created_date", "interpretant"]].groupby(['interpretant'])['interpretant'].count())
    
    print('Ok.')

    return

def extract_pages(thread_id):
    param = {
        "api_key": secret['API_KEY'],
        "thread": thread_id,
        "limit": 100,
        "order": "asc"
    }
    
    page = 1
    while True:
        print('Buscando página {}...'.format(page))
        response = requests.get("https://disqus.com/api/3.0/posts/list.json", param)
        response_body = response.json()
        if response.status_code == 200:
            yield response_body['response']
            print('Página completada.')
            
            page = page + 1
            if response_body['cursor']['hasNext'] == True:
                # Prepare to fetch the next page on next loop
                param['cursor'] = response_body['cursor']['next']
            else:
                break
        else:
            print('Erro na página!')
            break

def extract_post(body):
    for post in body:
        yield {
            "id": post['id'],
            "thread": post['thread'],
            "parent": post['parent'],
            "created_at": post['createdAt'],
            "message": post['raw_message'],
            "media": post['media'],
            "likes": post['likes'],
            "dislikes": post['dislikes'],
            "author": {
                "user": post['author']['username'],
                "name": post['author']['name'],
                "bio": post['author']['about'],
                "url": post['author']['profileUrl']
            }
        }

def fetch_posts(connection, cursor, thread_id):
    for page in extract_pages(thread_id):
        for post in extract_post(page):
            data = (
                post['id'],             # id integer
                post['thread'],         # thread integer,
                post['parent'],         # parent integer,
                post['created_at'],     # created_at text, 
                post['message'],        # message text,
                str(post['media']).strip('[]'), # media text,
                post['likes'],          # likes integer,
                post['dislikes'],       # dislikes integer,
                post['author']['user'], # author_user text,
                post['author']['name'], # author_name text,
                post['author']['bio'],  # author_bio text,
                post['author']['url'],  # author_url text
                ''                      # interpretant text
            )
            
            cursor.execute('INSERT INTO posts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)', data)

def opt_export(connection, cursor):
    data = opt_thread(connection, cursor)
    thread_num = int(input('Digite o Num da thread desejada: '))
    thread_id = data[thread_num][1]
    print('Opções\n\t1 - Exportar para excel (classificar)\n\t2 - Exportar para csv (pandas)')
    while(True):
        option = input('> ')

        if option == '1': 
            # Fetch data
            rows = cursor.execute('SELECT * FROM posts WHERE thread = {} AND parent IS NULL'.format(thread_id)).fetchall()
            excel_data = [list(elem) for elem in rows]

            # Export to Excel
            
            wb = Workbook()
            ws = wb.active
            
            ws.append(['id', 
                'thread', 
                'parent', 
                'created_at', 
                'message', 
                'media',
                'likes',
                'dislikes',
                'author_user',
                'author_name',
                'author_bio',
                'author_url'
            ])

            for row in excel_data:
                row[3] = datetime.strptime(row[3], '%Y-%m-%dT%H:%M:%S') # 2020-09-22T12:32:03
                ws.append(row)
            
            wb.save("{}.xlsx".format(thread_id))
            
            break
        elif option == '2':
            print('Num 2')
            break
        else:
            print('Num 4')
            pass
    print('Exportação concluída')
    return

if __name__ == "__main__":
    connection, cursor = check_and_connect('amigurumi.db')
    while(True):
        user = input('> ')
        
        if user in ['.quit', '.sair']:
            # exit gracefully
            cursor.close()
            connection.close()
            sys.exit('Bye!')
        
        elif user == '.download':
            opt_download(connection, cursor)

        elif user == '.thread':
            opt_thread(connection, cursor)

        elif user == '.interpretants':
            pass
        elif user == '.export':
            opt_export(connection, cursor)

        elif user == '.graph':
            opt_graph()

        else: 
            print('Opções:\n\t.download\n\t.thread\n\t.interpretants\n\t.export\n\t.graph')

    