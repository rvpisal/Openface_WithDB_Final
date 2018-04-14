# -*- coding: UTF-8 -*-

"""
@file db_manager.py
@brief
    Implement of database manager in database.

Created on: 2016/1/14
"""

import os
import sqlite3
import logging
import time
import datetime
from datetime import datetime,timedelta
import calendar

import faceapi
from faceapi import exceptions
from faceapi.utils import log_center
from faceapi.database import DbManager

"""
8888888b.            .d888 d8b
888  "Y88b          d88P"  Y8P
888    888          888
888    888  .d88b.  888888 888 88888b.   .d88b.  .d8888b
888    888 d8P  Y8b 888    888 888 "88b d8P  Y8b 88K
888    888 88888888 888    888 888  888 88888888 "Y8888b.
888  .d88P Y8b.     888    888 888  888 Y8b.          X88
8888888P"   "Y8888  888    888 888  888  "Y8888   88888P'
"""


_DB_FILE = os.path.join(faceapi.BASE_DIR, "data", "facedb.db3")
_SQL_CMD_CREATE_TAB = "CREATE TABLE IF NOT EXISTS "
_SQL_TABLE_FACE = (
    "face_table(hash TEXT PRIMARY KEY, "
    "name TEXT, "
    "eigen TEXT, "
    "src_hash TEXT, "
    "face_img TEXT, "
    "class_id INTEGER)")
_SQL_GET_ALL_FACE = "SELECT * FROM face_table"
_SQL_ROWS = "SELECT COUNT(*) FROM face_table"
_SQL_ADD_FACE = (
    "INSERT or REPLACE INTO "
    "face_table "
    "VALUES(?, ?, ?, ?, ?, ?)")
_SQL_GET_FACE_WITH_FIELD = "SELECT * FROM face_table WHERE {}={} LIMIT {}"
_SQL_DISTINCT_SEARCH = "select distinct {} from face_table order by {}"


"""
.d8888b.  888
d88P  Y88b 888
888    888 888
888        888  8888b.  .d8888b  .d8888b
888        888     "88b 88K      88K
888    888 888 .d888888 "Y8888b. "Y8888b.
Y88b  d88P 888 888  888      X88      X88
 "Y8888P"  888 "Y888888  88888P'  88888P'
 """


class DbManagerOpenface(DbManager):
    def __init__(self, db_file=_DB_FILE):
        super(DbManagerOpenface, self).__init__(db_file)
        self._db_file = db_file
        self._log = log_center.make_logger(__name__, logging.INFO)
        self._log.info("db_file: {}".format(db_file))

        dir = os.path.dirname(db_file)
        if not os.path.exists(dir):
            os.makedirs(dir)

        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cur.execute(_SQL_CMD_CREATE_TAB + _SQL_TABLE_FACE)
                db.commit()
        except sqlite3.Error as e:
            self._log.error(str(e))

    def count(self):
        rows = 0
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cur.execute(_SQL_ROWS)
                # result = cur.fetchone()
                # rows = result[0]
                (rows, ) = cur.fetchone()
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def dbList(self):
        rows = []
        db = None
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cur.execute(_SQL_GET_ALL_FACE)
                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def addList(self, record_list):
        if type(record_list) is not list:
            self._log.error("record_list is not a list type, do nothing.")
            return

        try:
            db = sqlite3.connect(self._db_file)
            cur = db.cursor()
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise exceptions.LibError(str(e))

        sql_add_list = []
        for record in record_list:
            rep_str = ",".join(str(x) for x in record.eigen)
            info = (
                record.hash, record.name, rep_str,
                record.src_hash, record.face_img, record.class_id)
            self._log.debug("add: " + str(info))
            sql_add_list.append(info)

        try:
            cur.executemany(_SQL_ADD_FACE, sql_add_list)
        except sqlite3.Error as e:
            self._log.error(str(e))

        db.commit()
        db.close()

    def search(self, field, value, count):
        rows = []
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_GET_FACE_WITH_FIELD.format(field, value, count)
                self._log.debug("sql cmd: {}".format(cmd))
                cur.execute(cmd)

                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def distinct_search(self, field_list, order_field):
        rows = []
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_DISTINCT_SEARCH.format(
                    ','.join(field_list), order_field)
                self._log.debug("sql cmd: {}".format(cmd))
                cur.execute(cmd)

                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    # def create_unique_db(self, name):
    #     # conn = _sqlite3.connect(".db3")
    #     time_stamp = sqlite3.datetime.datetime.now()
    #
    #     db_file = os.path.join(faceapi.BASE_DIR, "../web", "detected_faces.db3")
    #
    #     dir = os.path.dirname(db_file)
    #     if not os.path.exists(dir):
    #         os.makedirs(dir)
    #
    #
    #     try:
    #         with sqlite3.connect(db_file) as db:
    #             cur = db.cursor()
    #             cur.execute("Create table if not exists Names (name TEXT, Timestamp DATETIME)")
    #             db.commit()
    #             # cur.execute("select count(*) from (select name from Names where name =?)", ((hit['name']),))
    #             cur2 = db.cursor()
    #             # self._log.info(name)
    #             cur2.execute("select count(*) from (select name from Names where name =?)", (name,))
    #             name_counter = cur2.fetchone()
    #
    #             # self._log.info(name_counter)
    #             if name_counter == (0,):
    #                 # cur.execute("Insert into Names values(?,?)", (hit['name'], time_stamp))
    #                 cur2.execute("Insert into Names values(?,?)", (name, time_stamp))
    #                 db.commit()
    #             else:
    #                 # cur.execute("update Names set Timestamp=? where name=?", (time_stamp, (hit['name']),))
    #                 cur2.execute("update Names set Timestamp=? where name=?", (time_stamp, name))
    #                 db.commit()
    #             cur.close()
    #             cur2.close()
    #     except sqlite3.Error as e:
    #         self._log.error(str(e))
    #         raise e

    def create_unique_db(self, name):
        # conn = _sqlite3.connect(".db3")
        time_stamp = datetime.now()
        curr_date = datetime.date(datetime.now())

        db_file = os.path.join(faceapi.BASE_DIR, "../web", "Attendance.db3")

        dir = os.path.dirname(db_file)
        if not os.path.exists(dir):
            os.makedirs(dir)


        try:
            with sqlite3.connect(db_file) as db:

                db.execute("CREATE TABLE if not exists Attendance(Course_id INTEGER,ULID TEXT, TimeStamp TEXT, Day TEXT,"
                           " PRIMARY KEY(Course_id,Day))")
                db.execute("CREATE TABLE if not exists Class_Schedule (Course_id INTEGER, Subject_name text, Day Text, "
                            "Start_Time date, End_Time date, PRIMARY KEY(Course_id,Day))")
                db.execute("CREATE TABLE if not exists Registration_Details (ULID TEXT, Course_id INTEGER, "
                           "PRIMARY KEY(ULID,Course_id))")

                cur = db.cursor()

                cur.execute("Select Course_id from Registration_Details where ULID = ?", (name,))

                reg_classes = list([x[0] for x in cur.fetchall()])

                self._log.info(name)

                self._log.info(reg_classes)
                self._log.info(time_stamp)

                params = '?'*len(reg_classes)

                # print("Select Course_id from Class_Schedule where ? between Start_Time and End_Time and Day = ? and Course_id in ({})".format(','.join(params)))

                courses_taken =  "Select Course_id from Class_Schedule where time(?) between time(Start_Time) and time(End_Time) and Day = ? and " \
                                 "Course_id in ({})".format(','.join(params))


                query_args = [time_stamp, calendar.day_name[(time_stamp).weekday()]]
                query_args.extend(reg_classes)

                cur.execute(courses_taken,query_args)
                course_id = list ([x[0] for x in cur.fetchall()])


                # classes = list([x[0] for x in cur.fetchall()])
                # print ("Ongoing Classes: ", classes)
                # self._log.info(classes)

                # cur.execute("Select Course__id from Registration_Details where ULID = ?", (name,))
                #
                # reg_classes = list([x[0] for x in cur.fetchall()])
                # print ("Registered Classes: ",reg_classes)
                # self._log.info(reg_classes)

                # sub_id = list(set(classes).intersection(reg_classes))
                self._log.info(course_id)

                if course_id:
                    cur.execute("Select count(*) from (Select * from Attendance where course_id = ? and ULID = ? and "
                                "date(TimeStamp) = ?)", (course_id[0], name, curr_date))
                    attendance_counter = cur.fetchone()
                    if attendance_counter == (0,):
                        cur.execute("Insert into Attendance values(?,?,?,?)",
                                    (course_id[0], name, time_stamp, calendar.day_name[(time_stamp).weekday()]))
                        db.commit()
                    else:
                        pass

                db.commit()
                cur.close()
                # db.close()

        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e
