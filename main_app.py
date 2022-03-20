from flask import Flask, jsonify, request, Response, stream_with_context
import json
from flaskext.mysql import MySQL

app = Flask(__name__)
mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'sagar24'
app.config['MYSQL_DATABASE_PASSWORD'] = 'NBRoot@00'
app.config['MYSQL_DATABASE_DB'] = 'sagar24$bookstore'
app.config['MYSQL_DATABASE_HOST'] = 'sagar24.mysql.pythonanywhere-services.com'
mysql.init_app(app)


@app.route("/get_books", methods=['POST'])
def get_books():
    try:
        query = create_query(request.json)

        print('query :', query)

        with mysql.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)

            def generate_rows(db_cursor):
                books = []
                row_headers = [x[0] for x in cursor.description]
                rows = cursor.fetchmany(25)
                if len(rows):
                    for row in rows:
                        json_row = dict(zip(row_headers, row))
                        json_row['genre'] = json_row['bookshelf']
                        books.append(json_row)

                    yield json.dumps({"count" : db_cursor.rowcount, "books":books})
                else:
                    return jsonify({"count": 0, "books": 'No record found'})

            if cursor.rowcount > 25:
                return Response(stream_with_context(generate_rows(cursor)), mimetype='application/json')
            else:
                return jsonify({"count" : cursor.rowcount, "books":cursor.fetchall()})

    except Exception as exp:
        print(exp)


def create_query(query_payload):
    query = f"SELECT books_book.title as title, books_author.name as author_name, books_author.birth_year as birth_year, books_author.death_year as death_year, books_bookshelf.name as bookshelf, books_language.code as language_code, books_subject.name as subject, books_format.url as download_url" \
           f" FROM books_book" \
           f" INNER JOIN books_book_languages bbl ON books_book.id = bbl.book_id" \
           f" INNER JOIN books_language ON books_language.id = bbl.language_id" \
           f" INNER JOIN books_format ON books_book.id = books_format.book_id" \
           f" INNER JOIN books_book_bookshelves bbb ON books_book.id = bbb.book_id" \
           f" INNER JOIN books_bookshelf ON books_bookshelf.id = bbb.bookshelf_id" \
           f" INNER JOIN books_book_subjects bbs ON books_book.id = bbs.book_id" \
           f" INNER JOIN books_subject ON books_subject.id = bbs.subject_id" \
           f" INNER JOIN books_book_authors bba ON books_book.id = bba.book_id" \
           f" INNER JOIN books_author ON books_author.id = bba.author_id"

    filters = []

    if 'gutenbergId' in query_payload and len(query_payload['gutenbergId']):
        gutenberg_ids = ",".join([str(id) for id in query_payload['gutenbergId']])
        filters.append(f" books_book.gutenberg_id in ({gutenberg_ids})")

    if 'lang' in query_payload and len(query_payload['lang']):
        data = [f"'{lang}'" for lang in query_payload['lang']]
        languages = ",".join(data)
        filters.append(f" books_language.code in ({languages})")

    if 'mimeType' in query_payload and len(query_payload['mimeType']):
        data = [f"'{mime_type}'" for mime_type in query_payload['mimeType']]
        mime_types = ",".join(data)
        filters.append(f" books_format.mime_type in ({mime_types})")

    if 'topic' in query_payload and len(query_payload['topic']):
        topics = " AND ".join([f"(books_bookshelf.name LIKE '%{topic}%' OR books_subject.name LIKE '%{topic}%')" for topic in query_payload['topic']])
        filters.append(topics)

    if 'author' in query_payload and len(query_payload['author']):
        authors = " AND ".join([f"books_author.name LIKE '%{author}%'" for author in query_payload['author']])
        filters.append(authors)

    if 'title' in query_payload and len(query_payload['title']):
        titles = " AND ".join([f"books_book.title LIKE '%{title}%'" for title in query_payload['title']])
        filters.append(titles)

    if filters:
        query += " WHERE " + (" AND ".join(filters))

    query += " ORDER BY books_book.download_count DESC"

    print("final query: ", query)
    return query


if __name__ == "__main__":
    app.run(debug=True)
