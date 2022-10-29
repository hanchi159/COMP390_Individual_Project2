import requests
import json
import sqlite3

def convert_dict_to_text(record):
    if record.get('geolocation', None) is None:
        return None
    return json.dumps(record['geolocation'])

def convert_obj_to_string(dict_record, key):
    """ <dict>.get() gets the value associated with a key in a dictionary
        the second parameter for <dict>.get() tells the get function to return 'None' if the key does not exist. """
    if dict_record.get(key, None) is None:
        return None
    # return a string version of the <dict>[key] object if a key is present
    return json.dumps(dict_record[key])

def issue_get_request(url):
    """ This function issues a GET request to the URL passed in as its single parameter.
        A sqlite3 Response object is returned.
        The status code of the Response is also reported. """

    response_obj = requests.get(url)
    if response_obj.status_code != 200:
        print(f'The GET request was NOT successful\n{response_obj.status_code} [{response_obj.reason}]\n')
    else:
        print(f'The GET request was successful\n{response_obj.status_code} [{response_obj.reason}]\n')
    return response_obj

def convert_content_to_json(response_obj: requests.Response):
    """ This function accepts a repeat Response object as its single parameter and try to convert
        the Response object's content to a JSON data object
        'None' is returned if the conversion was unsuccessful. """

    json_data_obj = None

    try:
        json_data_obj = response_obj.json()
        print(f'Response object content converted to JSON object.\n')
    except requests.exceptions.JSONDecodeError as json_decode_error:
        print(f'An error occurred while trying to convert the response content to a JSON objet:\n'
              f'{json_decode_error}')
    finally:
        return json_data_obj

def establish_database_connection(database_name: str):
    """This function tries to establish a connection with a SQLite database.
        A database connection object is returned if the connection was successful.
        'None' is returned if the connection was unable to ve established. """

    db_connection = None

    try:
        db_connection = sqlite3.Connection(database_name)
        print(f'Connection to database \'{database_name}\' was established successfully.\n'
              f'Database connection object: {db_connection}\n')

    except sqlite3.Error as db_error:
        print(f'An error occurred while trying to connect to database {database_name}:\n'
              f'{db_error}\n')
    finally:
        return db_connection

def main():
    """ Execute functions """
    target_url = 'https://data.nasa.gov/resource/gh4g-9sfh.json'
    get_req_response = issue_get_request(target_url)

    json_data = convert_content_to_json(get_req_response)

    db_connection_obj = establish_database_connection('continental_region_meteorites.db')

    """CREATE "Meteorite_Data" (main data) TABLE and INSERT datas ON the TABLE"""
    try:

        # create a cursor object
        db_cursor = db_connection_obj.cursor()

        # create a table in the database to store all of your meteorite entries
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Meteorite_Data(
                                name TEXT,
                                id TEXT PRIMARY KEY,
                                nametype TEXT,
                                recclass TEXT,
                                mass TEXT,
                                fall TEXT,
                                year TEXT,
                                reclat FLOAT,
                                reclong FLOAT,
                                geolocation TEXT,
                                states TEXT,
                                counties TEXT);''')

        #clear the table if there is already data in it
        db_cursor.execute('DELETE FROM Meteorite_Data')

        for record in json_data:
            db_cursor.execute('''INSERT INTO Meteorite_Data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (record.get('name', None),
                               int(record.get('id', None)),
                               record.get('nametype', None),
                               record.get('recclass', None),
                               record.get('mass', None),
                               record.get('fall', None),
                               record.get('year', None),
                               record.get('reclat', None),
                               record.get('reclong', None),
                               convert_obj_to_string(record, 'geolocation'),  # convert geolocation <dict> to string
                               record.get(':@computed_region_cbhk_fwbd', None),
                               record.get(':@computed_region_nnqa_25f4', None)))

        db_connection_obj.commit()
    except sqlite3.Error as db_error:
        print(f'The Meteorite_Data Data Base Error has occurred: {db_error}')

    try:
        """Create Africa_MiddleEast_Meteorites table"""
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Africa_MiddleEast_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat FLOAT,
                                reclong FLOAT);''')

        # clear the table if there is already data in it
        db_cursor.execute('DELETE FROM Africa_MiddleEast_Meteorites')
        # select entries and data from main data for Africa_MiddleEast_Meteorites
        db_cursor.execute('''SELECT name, mass, reclat, reclong FROM Meteorite_Data WHERE ((reclat >= -35.2 AND reclat <= 37.6) AND (reclong >= -17.8 AND reclong <= 62.2))''')

        result = db_cursor.fetchall()
        # print(result) --> tuple
        for filtered_entry in range(len(result)):
            db_cursor.execute('''INSERT INTO Africa_MiddleEast_Meteorites VALUES(?,?,?,?)''',
                              (result[filtered_entry][0],
                               result[filtered_entry][1],
                               result[filtered_entry][2],
                               result[filtered_entry][3]))
        db_connection_obj.commit()

    except sqlite3.Error as db_error:
        print(f'The Africa_MiddleEast_Meteorites Data Base Error has occurred: {db_error}')

    try:
        '''Create Europe_Meteorites table'''
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Europe_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat FLOAT,
                                reclong FLOAT);''')
        # clear the table if there is already data in it
        db_cursor.execute('DELETE FROM Europe_Meteorites')
        # select entries and data from main data for Europe_Meteorites
        db_cursor.execute('''SELECT name, mass, reclat, reclong FROM Meteorite_Data WHERE (reclat >= 36 AND reclat <= 71.1) AND (reclong >= -24.1 AND reclong <= 32)''')

        result = db_cursor.fetchall()
        # print(result) --> tuple
        for filtered_entry in range(len(result)):
            db_cursor.execute('''INSERT INTO Europe_Meteorites VALUES(?,?,?,?)''',
                              (result[filtered_entry][0],
                               result[filtered_entry][1],
                               result[filtered_entry][2],
                               result[filtered_entry][3]))
        db_connection_obj.commit()
    except sqlite3.Error as db_error:
        print(f'The Europe_Meteorites Data Base Error has occurred: {db_error}')

    try:
        '''Create Upper_Asia_Meteorites table'''
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Upper_Asia_Meteorites(
                                name TEXT,
                                mass TEXT,
                                reclat FLOAT,
                                reclong FLOAT);''')
        # clear the table if there is already data in it
        db_cursor.execute('DELETE FROM Upper_Asia_Meteorites')
        # select entries and data from main data for Upper_Asia_Meteorites
        db_cursor.execute('''SELECT name, mass, reclat, reclong FROM Meteorite_Data WHERE ((reclat >= 35.8 AND reclat <= 72.7) AND (reclong >= 32.2 AND reclong <= 190.4))''')

        result = db_cursor.fetchall()
        # print(result) --> tuple
        for filtered_entry in range(len(result)):
            db_cursor.execute('''INSERT INTO Upper_Asia_Meteorites VALUES(?,?,?,?)''',
                              (result[filtered_entry][0],
                               result[filtered_entry][1],
                               result[filtered_entry][2],
                               result[filtered_entry][3]))
        db_connection_obj.commit()
    except sqlite3.Error as db_error:
        print(f'The Upper_Asia_Meteorites Data Base Error has occurred: {db_error}')

    try:
        '''Create Lower_Asia_Meteorites table'''
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Lower_Asia_Meteorites(
                                        name TEXT,
                                        mass TEXT,
                                        reclat FLOAT,
                                        reclong FLOAT);''')
        # clear the table if there is already data in it
        db_cursor.execute('DELETE FROM Lower_Asia_Meteorites')
        # select entries and data from main data for Lower_Asia_Meteorites
        db_cursor.execute('''SELECT name, mass, reclat, reclong FROM Meteorite_Data WHERE ((reclat >= -9.9 AND reclat <= 38.6) AND (reclong >= 58.2 AND reclong <= 154))''')

        result = db_cursor.fetchall()
        # print(result) --> tuple
        for filtered_entry in range(len(result)):
            db_cursor.execute('''INSERT INTO Lower_Asia_Meteorites VALUES(?,?,?,?)''',
                              (result[filtered_entry][0],
                               result[filtered_entry][1],
                               result[filtered_entry][2],
                               result[filtered_entry][3]))
        db_connection_obj.commit()
    except sqlite3.Error as db_error:
        print(f'The Lower_Asia_Meteorites Data Base Error has occurred: {db_error}')

    try:
        '''Create Australia_Meteorites table'''
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Australia_Meteorites(
                                                name TEXT,
                                                mass TEXT,
                                                reclat FLOAT,
                                                reclong FLOAT);''')
        # clear the table if there is already data in it
        db_cursor.execute('DELETE FROM Australia_Meteorites')
        # select entries and data from main data for Australia_Meteorites
        db_cursor.execute('''SELECT name, mass, reclat, reclong FROM Meteorite_Data WHERE (reclat >= -43.8 AND reclat <= -11.1) AND (reclong >= 112.9 AND reclong <= 154.3)''')

        result = db_cursor.fetchall()
        # print(result) --> tuple
        for filtered_entry in range(len(result)):
            db_cursor.execute('''INSERT INTO Australia_Meteorites VALUES(?,?,?,?)''',
                              (result[filtered_entry][0],
                               result[filtered_entry][1],
                               result[filtered_entry][2],
                               result[filtered_entry][3]))
        db_connection_obj.commit()
    except sqlite3.Error as db_error:
        print(f'The Australia_Meteorites Data Base Error has occurred: {db_error}')

    try:
        '''Create North_America_Meteorites table'''
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS North_America_Meteorites(
                                                        name TEXT,
                                                        mass TEXT,
                                                        reclat FLOAT,
                                                        reclong FLOAT);''')
        # clear the table if there is already data in it
        db_cursor.execute('DELETE FROM North_America_Meteorites')
        # select entries and data from main data for North_America_Meteorites
        db_cursor.execute('''SELECT name, mass, reclat, reclong FROM Meteorite_Data WHERE (reclat >= 12.8 AND reclat <= 71.5) AND (reclong >= -168.2 AND reclong <= -52)''')

        result = db_cursor.fetchall()
        # print(result) --> tuple
        for filtered_entry in range(len(result)):
            db_cursor.execute('''INSERT INTO North_America_Meteorites VALUES(?,?,?,?)''',
                              (result[filtered_entry][0],
                               result[filtered_entry][1],
                               result[filtered_entry][2],
                               result[filtered_entry][3]))
        db_connection_obj.commit()
    except sqlite3.Error as db_error:
        print(f'The North_America_Meteorites Data Base Error has occurred: {db_error}')

    try:
        '''Create South_America_Meteorites table'''
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS South_America_Meteorites(
                                                                name TEXT,
                                                                mass TEXT,
                                                                reclat FLOAT,
                                                                reclong FLOAT);''')
        # clear the table if there is already data in it
        db_cursor.execute('DELETE FROM South_America_Meteorites')
        # select entries and data from main data for South_America_Meteorites
        db_cursor.execute('''SELECT name, mass, reclat, reclong FROM Meteorite_Data WHERE (reclat >= -55.8 AND reclat <= 12.6) AND (reclong >= -81.2 AND reclong <= -34.4)''')

        result = db_cursor.fetchall()
        # print(result) --> tuple
        for filtered_entry in range(len(result)):
            db_cursor.execute('''INSERT INTO South_America_Meteorites VALUES(?,?,?,?)''',
                              (result[filtered_entry][0],
                               result[filtered_entry][1],
                               result[filtered_entry][2],
                               result[filtered_entry][3]))
        db_connection_obj.commit()

    except sqlite3.Error as db_error:
        print(f'The South_America_Meteorites Data Base Error has occurred: {db_error}')

    # close the database whether an error occurred or not
    finally:
        if db_connection_obj:
            db_connection_obj.close()
            print("database has been closed")

if __name__ == '__main__':
    main()