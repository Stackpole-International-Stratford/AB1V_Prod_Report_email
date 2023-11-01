import re
import sys
from datetime import datetime, timedelta
import mysql.connector
import jinja2

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

logger.debug("Running")

email_config = {
    'server': 'mesg06.stackpole.ca',
    'from': 'cstrutton@stackpole.com',
    'to': [
        'dbrenneman@stackpole.com',
        'rzylstra@stackpole.com',
        'dmilne@stackpole.com',
        'lbaker@stackpole.com',
        'jmcmaster@stackpole.com',
        'roberto.jimenez@vantage-corp.com',
        'cstrutton@stackpole.com',
    ],
    'subject': 'AB1V Autogauge scrap report'
}

db_config = {
    'user': os.getenv('DB_USER', 'stuser'),
    'password': os.getenv('DB_PASSWORD', 'stp383'),
    'host': os.getenv('DB_HOST', '10.4.1.245'),
    'port': os.getenv('DB_PORT', '3306'),
    'database': 'prodrptdb',
    'raise_on_warnings': True
}


def get_part_list(range_start, range_end=None):
    if not range_end:
        range_end = range_start + timedelta(hours=24)
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()

    query = ("SELECT DISTINCT part_number FROM 1730_Vantage "
             "WHERE created_at BETWEEN %s AND %s")

    cursor.execute(query, (range_start, range_end))
    parts = []
    for row in cursor:
        # print(row[0])
        parts.append(row[0])

    cursor.close()
    cnx.close()
    return parts


def good_part_count(part_number, start_date, end_date):
    if not end_date:
        end_date = start_date + timedelta(hours=24)
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()

    query = ("SELECT COUNT(*) FROM 1730_Vantage "
             "WHERE part_number = %s "
             "AND part_fail = 1 "
             "AND (created_at BETWEEN %s AND %s)")

    cursor.execute(query, (part_number, start_date, end_date))
    res = cursor.fetchone()

    cursor.close()
    cnx.close()

    return res[0]


def shift_times(date, date_offset=0):
    # end_date is this morning at 7am
    end_date = date.replace(hour=7, minute=0, second=0, microsecond=0)
    # adjust end_date by date_offset days
    end_date = end_date - timedelta(days=date_offset)
    # start_date is yesterday morning at 7am
    start_date = end_date - timedelta(hours=24)
    end_date = end_date - timedelta(seconds=1)
    return start_date, end_date


def reject_part_count(part_number, start_date, end_date):
    # initialize empty results object
    results = {
        'spotface': {'label': 'Spot Face', 'count': 0},
        'media': {'label': 'Media', 'count': 0},
        'oilholes': {'label': 'Oil Holes', 'count': 0},
        'induction': {'label': 'Induction', 'count': 0},
        'balpos': {'label': 'Balance Pos', 'count': 0},
        'balwitness': {'label': 'Bal Witness Mark', 'count': 0},
        'winheight': {'label': 'Window Height', 'count': 0},
        'staking': {'label': 'Staking Pocket', 'count': 0},
        'pocketholes': {'label': 'Mach Pocket Holes', 'count': 0},
        'eddy': {'label': 'Eddy Current', 'count': 0},
        'res': {'label': 'Resonance', 'count': 0},
        'plateph': {'label': 'Plate PH', 'count': 0},
        'pedph': {'label': 'Ped PH', 'count': 0},
        'bushid': {'label': 'Bush ID', 'count': 0},
        'upid': {'label': 'Upper ID', 'count': 0},
        'lowerid': {'label': 'Lower ID', 'count': 0},
        'other': {'label': 'Other', 'count': 0}
    }

    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()

    query = ("SELECT inspection_data FROM 1730_Vantage "
             "WHERE part_number = %s "
             "AND part_fail = 2 "
             "AND (created_at BETWEEN %s AND %s)")

    cursor.execute(query, (part_number, start_date, end_date))

    for row in cursor:
        features = re.split(r'\t+', row[0])
        try:
            failure = features.index("FAIL")
        except ValueError:
            failure = -1

        if failure == 8:  # Spot face check
            results['spotface']['count'] += 1

        # 5 - Media presence- Braze pellet holes
        elif failure == 5:
            results['media']['count'] += 1
        # 7 - Media presence- Pellet holes
        elif failure == 7:
            results['media']['count'] += 1
        # 12 - Media presence bal hole
        elif failure == 12:
            results['media']['count'] += 1
        # 14 - Media presence pinion holes
        # 15 - Media presence web slot
        # 16 - Media presence windows
        # 17 - Media presence Machined Recess plate side
        # 18 - Media presence blind ped. Holes
        # 19 - Media presence slot at pinion hole
        # 20 - Media presence blind pellet holes
        elif 14 <= failure <= 20:
            results['media']['count'] += 1
        # 35 - Media presence Machined recess pedestal side
        elif failure == 35:
            results['media']['count'] += 1

        # 6 - Lube holes
        elif failure == 6:
            results['oilholes']['count'] += 1
        # 9 - Pinion crosshole presence
        elif failure == 9:
            results['oilholes']['count'] += 1

        # 10 - Induction hardening presence
        elif failure == 10:
            results['induction']['count'] += 1

        # 11 - Balance hole position
        elif failure == 11:
            results['balpos']['count'] += 1

        # 13 - Witness mark
        elif failure == 13:
            results['balwitness']['count'] += 1

        # 31 - new window height 'status'
        elif failure == 31:
            results['winheight']['count'] += 1

        # 34 - Staking pocket presence
        elif failure == 34:
            results['staking']['count'] += 1

        # 36 - Pedestal side Machined pocket holes
        elif failure == 36:
            results['pocketholes']['count'] += 1

        # 37 - Eddy Current Result
        elif failure == 37:
            results['eddy']['count'] += 1

        # 38 - Resonance Result
        elif failure == 38:
            results['res']['count'] += 1

        # 39-48 Plate Pinion Hole Status x5
        elif 39 <= failure <= 48:
            results['plateph']['count'] += 1

        # 49-58 Plate Pinion Hole Status x5
        elif 49 <= failure <= 58:
            results['pedph']['count'] += 1

        # 60 - Bushing ID status
        elif failure == 60:
            results['bushid']['count'] += 1

        # 62 - Upper ID status
        elif failure == 62:
            results['upid']['count'] += 1

        # 64 - Lower ID status
        elif failure == 64:
            results['lowerid']['count'] += 1

        else:
            results['other']['count'] += 1

    cursor.close()
    cnx.close()

    return results


def report_html(start, end):
    data = []
    part_list = get_part_list(start, end)
    for part in part_list:
        data.append({
            'part_number': part,
            'good': good_part_count(part, start, end),
            'reject': reject_part_count(part, start, end)
        })
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=''))
    template = env.get_template('template.html')
    return template.render(data=data, start=start, end=end)



def testout():
    parameters = [
        ("50-9341", 0, ['1533']),
        ("50-0455", 0, ['1812']),
        ("50-1467", 0, ['650L', '650R', '769']),
        ("50-3050", 0, ['769']),
        ("50-8670", 0, ['1724', '1725', '1750']),
        ("50-0450", 0, ['1724', '1725', '1750']),
        ("50-5401", 0, ['1724', '1725', '1750']),
        ("50-0447", 0, ['1724', '1725', '1750']),
        ("50-5404", 0, ['1724', '1725', '1750']),
        ("50-0519", 0, ['1724', '1725', '1750']),
        ("50-4865", 0, ['1617']),
        ("50-5081", 0, ['1617']),
        ("50-4748", 0, ['797']),
        ("50-3214", 0, ['1725']),
        ("50-5214", 0, ['1725']),
    ]

    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    start_date = '2023-10-2'
    end_date = '2023-10-8'
    part_amount_list = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    part_index = 0

    for row in parameters:
        part = row[0]
        machine_list = row[2]
        for machine in machine_list:
            query = ("SELECT CAST(SUM(Count) as INTEGER) FROM GFxPRoduction "
             "WHERE Part = %s "
             "AND Machine = %s "
             "AND `TimeStamp` > UNIX_TIMESTAMP(DATE(%s)) AND UNIX_TIMESTAMP(DATE(%s));")
            
            cursor.execute(query, (part, machine, start_date, end_date))
            #SELECT `TimeStamp`, SUM(Count) FROM GFxPRoduction
#WHERE `TimeStamp` > UNIX_TIMESTAMP(DATE('2023-10-02'))
#AND `TimeStamp` < UNIX_TIMESTAMP(DATE('2023-10-08')) LIMIT 10;
            for x in cursor:
                if x[0] != None:
                    part_amount_list[part_index] += x[0]
                #print(x[0])
            
        part_index += 1
        
            

    print(part_amount_list)
    query = ("SELECT Partlines.Line, SUM(tkb_scheduled.Hrs), tkb_scheduled.Part "
             "FROM tkb_scheduled "
             "LEFT JOIN PartLines "
             "ON PartLines.Part = tkb_scheduled.Part "
             "WHERE DATE(Date1) > DATE(%s) "
             "AND DATE(Date1) < DATE(%s) "
             "GROUP BY PartLines.Line, tkb_scheduled.Part;")
    cursor.execute(query, (start_date, end_date))

    for x in cursor:
        print(x)
    








    cursor.close()
    cnx.close()
    


    #for part in list
    #for machine in machine list
    #select SUM(count) where machine,part time range match
    

@logger.catch
def main():
    offset = 0 if len(sys.argv) == 1 else int(sys.argv[1])
    start_time, end_time = shift_times(datetime.now(), offset)
    report = report_html(start_time, end_time)
    message = MIMEMultipart("alternative")
    message["Subject"] = email_config['subject']
    message["From"] = email_config['from']
    message["To"] = ", ".join(email_config['to'])
    msg_body = MIMEText(report, "html")
    message.attach(msg_body)
    server = smtplib.SMTP(email_config['server'])
    server.sendmail(email_config['from'], email_config['to'], message.as_string())
    server.quit()


if __name__ == '__main__':
    testout()
