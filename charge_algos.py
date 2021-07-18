import random
import spidev
import redis
import datetime
import sqlite3
from sqlite3 import Error
import time

#SPI Configurations
def Start_Spi():
    bus = 0
    device = 0
    spi = spidev.SpiDev()
    spi.open(bus, device)
    spi.max_speed_hz = 5000
    spi.mode = 0b00
    return spi
    
def connect_redis():
    """
    connect ot redis database and gets the value from the database
    :return:
    """
    # connect to redis
    rdb = redis.Redis(host="127.0.0.1", port=6379, db=0)
    # sample a new value random times between 100ms and 2s
    mode_value = rdb.get('mode_type')
    print(str(mode_value))
    return mode_value


class connect_sqlite:
    """
    handles sqlite
    """

    def __init__(self):
        try:
            self.conn = sqlite3.connect('test.db',detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            print("Connected database successfully")
        except Exception as e:
            print(e)

    def create_table(self, tbl_name):
        """
        creates the table in a database in these case
        :param tbl_name: desire table name we wanted.
        :return: created table of desired table name
        """
        sql_query = '''CREATE TABLE IF NOT EXISTS {}(
                                        id integer PRIMARY KEY,
                                        Timestamp timestamp,
                                        V1 numeric,V2 numeric,V3 numeric,V4 numeric,V5 numeric,V6 numeric,V7 numeric,V8 numeric,V9 numeric,V10 numeric,
                                        T1 numeric,T2 numeric,T3 numeric,T4 numeric,T5 numeric,T6 numeric,T7 numeric,T8 numeric,T9 numeric,T10 numeric,
                                        C1 numeric,C2 numeric,C3 numeric,C4 numeric,C5 numeric,C6 numeric,C7 numeric,C8 numeric,C9 numeric,C10 numeric,
                                        I1 numeric,I2 numeric,I3 numeric,I4 numeric,I5 numeric,I6 numeric,I7 numeric,I8 numeric,I9 numeric,I10 numeric,
                                        A1 numeric,A2 numeric,A3 numeric,A4 numeric,A5 numeric,A6 numeric,A7 numeric,A8 numeric,A9 numeric,A10 numeric
                                    )'''.format(tbl_name)
        try:
            self.conn.execute(sql_query)
            print("sql table created!")
        except Exception as e:
            print(e)

    def read_table(self, tbl_name):
        """

        :param tbl_name: table name
        :return: returns the table objects
        """
        sql_query = '''SELECT  * FROM {}'''.format(tbl_name)
        try:
            return self.conn.execute(sql_query)
        except Exception as e:
            print(e)

    def write_table(self, tbl_name, val_list):
        """

        :param tbl_name:
        :param val_list:
        :return:
        """
        #print(val_list)
        sql_query = '''INSERT INTO  {}(Timestamp,V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,A1,A2,A3,A4,A5,A6,A7,A8,A9,A10)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''.format(tbl_name)
        #print(sql_query)
        try:
            self.conn.execute(sql_query, val_list)
            self.conn.commit()
            print("data entered into {} table successfully!".format(tbl_name))
        except Exception as e:
            print(e)

    def delete_table(self, tbl_name):
        sql_query = '''DROP TABLE {}'''.format(tbl_name)
        try:
            self.conn.execute(sql_query)
        except Exception as e:
            print(e)

    def show_tables(self):
        sql_query = '''SELECT name FROM sqlite_master WHERE type='table';'''
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_query)
            print(cursor.fetchall())
        except Exception as e:
            print(e)

    def close_connection(self):
        self.conn.close()


def table_name_generator():
    current_time = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
    # bytes to string
    current_mode = connect_redis().decode()
    if current_mode == 'Mode1':
        mode_name = 'Dummy'
    elif current_mode == 'Mode2':
        mode_name = 'CC_New_Battery'
    elif current_mode == 'Mode3':
        mode_name = 'Boost_Mode'
    elif current_mode == 'Mode4':
        mode_name = 'Float_Mode'
    elif current_mode == 'Mode5':
        mode_name = 'Trickle_Mode'
    else:
        mode_name = 'Invalid'
    # current mode + current date
    table_name = str(mode_name) + "_" + str(current_time)
    print(table_name)
    print(type(table_name))
    table_list(table_name)  # appends the list into queue for our reference
    return table_name


def table_list(table_name):
    queue = [table_name]
    if len(queue) == 25:
        queue.pop(0)


def get_timestamp():
    current_time = datetime.datetime.now()
    return current_time.timestamp()

def get_datetime():
   current_time = datetime.datetime.now()
   return current_time

def mode_one():
    print("hello mode one")
    """# read the in-memory database (redis)
    connect_redis()
    # create/connect the sqlite database
    data = connect_sqlite()
    data.create_table(table_name_generator())
    # insert the 100 rows into  table
    for i in range(100):
        # Generate 4 random numbers between 0 and 100
        random_list = random.sample(range(0, 100), 4)
        data.write_table(table_name_generator(), random_list)
    data.show_tables()"""




def mode_two():
    print("hello mode two")


def mode_three():
    print("hello mode three")

def Perform_OCV(spi, data, table_name):
    #OCV Block Start
    OCV_Command =[0x1]
    spi.writebytes(OCV_Command)
    #Dummy_Read = spi.readbytes(3)
    time.sleep(5)
    Read_BMS_Command = [0x2]
    spi.writebytes(Read_BMS_Command)
    #Dummy_Read = spi.readbytes(3)
    time.sleep(5)
    Dummy_Write = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    Rcvd_Data = spi.readbytes(100)
    time.sleep(0.5)
    Temporary_List = Rcvd_Data.copy()
    Temp_MSB=Temporary_List[0:len(Temporary_List):2]
    Temp_LSB=Temporary_List[1:len(Temporary_List):2]
    BMS_Data=[]
    i=0
    while i<50:
        Temp1 = Temp_MSB[i]
        Temp1 = Temp1<<8
        Temp2 = Temp_LSB[i]
        BMS_Data.insert(i,Temp1 + Temp2)
        i+=1
    #Get Timestamp
    Temp = BMS_Data.copy()
    Time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #Time = datetime.datetime.now().replace(microsecond=0)
    BMS_Data.insert(0, Time)
    BMS_Store_Data = tuple(BMS_Data)
    
    #Store Data onto Database
    data.write_table(table_name, BMS_Store_Data)
    print(BMS_Data)
    #OCV Block End
    return Temp

def Read_BMS_Parameters(spi, data, table_name):
    #Coulumb_Counting Block Start
    Read_BMS_Command =[0x2]
    spi.writebytes(Read_BMS_Command)
    #Dummy_Read = spi.readbytes(3)
    time.sleep(5)
    Dummy_Write = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    Rcvd_Data = spi.readbytes(100)
    time.sleep(0.5)
    Temporary_List = Rcvd_Data.copy()
    Temp_MSB=Temporary_List[0:len(Temporary_List):2]
    Temp_LSB=Temporary_List[1:len(Temporary_List):2]
    BMS_Data=[]
    i=0
    while i<50:
        Temp1 = Temp_MSB[i]
        Temp1 = Temp1<<8
        Temp2 = Temp_LSB[i]
        BMS_Data.insert(i,Temp1 + Temp2)
        i+=1
    #Get Timestamp
    Temp = BMS_Data.copy()
    Time = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    #Time = datetime.datetime.now().replace(microsecond=0)
    BMS_Data.insert(0, Time)
    BMS_Store_Data = tuple(BMS_Data)
    #Store Data onto Database
    data.write_table(table_name, BMS_Store_Data)
    print(BMS_Data)
    return Temp

def Update_Charging_Parameters(spi,Charging_Voltage,Charging_Current):
    Temp1 = Charging_Voltage.to_bytes(2,'big')
    Temp2 = Charging_Current.to_bytes(2,'big')
    Charge_V = list(Temp1)
    Charge_C = list(Temp2)
    Terminating = [0xFE]
    #print(Charge_V)
    #print(Charge_C)
    #Update_Voltage_Command = 0x3
    #Update_Current_Command = 0x4
    Update_Voltage = [0x3]
    Update_Current = [0x4]
    Update_Voltage.extend(Charge_V)
    Update_Current.extend(Charge_C)
    Update_Voltage.extend(Terminating)
    Update_Current.extend(Terminating)
    print(Update_Voltage)
    print(Update_Current)
    #Update Voltage
    spi.writebytes(Update_Voltage)
    #D_Read = spi.readbytes(3)
    time.sleep(1)
    #Update Current
    spi.writebytes(Update_Current)
    #D_Read = spi.readbytes(3)

def Start_Charging(spi):
    Start_Charging_Command = [0x5]
    spi.writebytes(Start_Charging_Command)
    #D_Read = spi.readbytes(3)

def Stop_Charging(spi):
    Stop_Charging_Command = [0x6]
    spi.writebytes(Stop_Charging_Command)
    #D_Read = spi.readbytes(3)

"""def Alert():
    led = LED(17)
    i=0
    while i<3:
        led.on()
        time.sleep(0.5)
        led.off()
        time.sleep(0.5)
        i+=1"""

def Emergency_Stop_Routine(spi):
    Stop_Charging(spi)
    #led = OUTPUT_PIN(17)
    #while User_Acknowledge_Flag == 0:
       # led.on()

def Configuration(AH,Make):
    if AH == '0':
        if Make == '0':
            #Configuration for Jumbo 75AH
            Config[0] = 0 #Charging Voltage 
            Config[1] = 0 #Charging Cureent
            Config[2] = 1325 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 0 #Duration
        elif Make == '1':
            #Configuration for Celtek 75AH
            Config[0] = 0 #Charging Voltage 
            Config[1] = 0 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1325 #Full Charge Voltage High Limit
            Config[4] = 0 #Duration
        elif Make == '2':
            #Configuration for Microtek 75AH
            Config[0] = 0 #Charging Voltage 
            Config[1] = 0 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 0 #Duration
        elif Make == '3':
            #Configuration for Exide 75AH
            Config[0] = 0 #Charging Voltage 
            Config[1] = 0 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 0 #Duration
        elif Make == '4':
            #Configuration for Bharat 75AH
            Config[0] = 0 #Charging Voltage 
            Config[1] = 0 #Charging Cureent
            Config[2] = 1325 #Full Charge Voltage Low Limit
            Config[3] = 1375 #Full Charge Voltage High Limit
            Config[4] = 0 #Duration
        elif Make == '5':
            #Configuration for Southern 75AH
            Config[0] = 0 #Charging Voltage 
            Config[1] = 0 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 0 #Duration
        elif  Make == '6':
            #Configuration for Other 75AH
            Config[0] = 0 #Charging Voltage 
            Config[1] = 0 #Charging Cureent
            Config[2] = 1325 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 0 #Duration
    elif AH == '1':
        if Make == '0':
            #Configuration for Jumbo 90AH
            Config[0] =  1350 #Charging Voltage 
            Config[1] =  70 #Charging Cureent
            Config[2] =  1325 #Full Charge Voltage Low Limit
            Config[3] =  1250 #Full Charge Voltage High Limit
            Config[4] =  75 #Duration
        elif Make == '1':
            #Configuration for Celtek 90AH
            Config[0] = 1350 #Charging Voltage 
            Config[1] = 60 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1325 #Full Charge Voltage High Limit
            Config[4] = 75 #Duration
        elif Make == '2':
            #Configuration for Microtek 90AH
            Config[0] = 1350 #Charging Voltage 
            Config[1] = 60 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 75 #Duration
        elif Make == '3':
            #Configuration for Exide 90AH
            Config[0] = 1350 #Charging Voltage 
            Config[1] = 50 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 75 #Duration
        elif Make == '4':
            #Configuration for Bharat 90AH
            Config[0] = 1350 #Charging Voltage 
            Config[1] = 70 #Charging Cureent
            Config[2] = 1325 #Full Charge Voltage Low Limit
            Config[3] = 1375 #Full Charge Voltage High Limit
            Config[4] = 75 #Duration
        elif Make == '5':
            #Configuration for Southern 90AH
            Config[0] = 1350 #Charging Voltage 
            Config[1] = 40 #Charging Cureent
            Config[2] = 1300 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 75 #Duration
        elif  Make == '6':
            #Configuration for Other 90AH
            Config[0] = 1350 #Charging Voltage 
            Config[1] = 40 #Charging Cureent
            Config[2] = 1325 #Full Charge Voltage Low Limit
            Config[3] = 1350 #Full Charge Voltage High Limit
            Config[4] = 75 #Duration

    return Config




def Constant_Current_Charging():
    
    spi = Start_Spi()
    data = connect_sqlite()
    table_name = table_name_generator()
    data.create_table(table_name)
    
    #Push Table onto redis


    #End Block

    #Configueration Selection Block

    #Read AH value from redis
    AH_Value = '0'
    #Read Make value from redis
    Make_Value = '0'
    

    Config=Configuration(AH_Value,Mak_Value)
    
    
    Charging_Voltage_Predefined = Config[0]
    Charging_Current_Predefined = Config[1]
    Full_Charge_Voltage_L = Config[2]
    Full_Charge_Voltage_H = Config[3]
    Duration = Config[4]
    
    #End Configuration Block
    
    """
    Charging_Voltage_Predefined = 1350 #135.0V
    Charging_Current_Predefined = 40 #4.0A
    Charging_Voltage_Stopped = 0
    Charging_Current_Stopped = 0
    Full_Charge_Voltage_L = 1300 #13.00V
    Full_Charge_Voltage_H = 1350 #13.50V """
    
    #Duration = 75
    Charging_Voltage_Stopped = 0
    Charging_Current_Stopped = 0
    Fault_Voltage_Level= 1255
    High_Temperature_Level = 4000
    Critical_Temperature_Level = 4500
    #Exit_Flag = 0
    Critical_Alarm_Flag = 0
    Current_Reuction_Flag = 0
    Fault=[0,0,0,0,0,0,0,0,0,0]
    
    """spi = Start_Spi()
    data = connect_sqlite()
    table_name = table_name_generator()
    data.create_table(table_name)
    
    #Push Table onto redis


    #End Block"""


    Start_Time = datetime.datetime.now().timestamp()
    #Loop_Time = Start_Time + ((Duration - 3) * 3600)
    #Max_Time = Start_Time + (Duration * 3600)
    Loop_Time = Start_Time + (60) #For Test
    Max_Time = Start_Time + (30)  #For Test
    
    #redis mode reset to prevent rerun of code
    rdb = redis.Redis(host="127.0.0.1", port=6379, db=0)
    mode_select = 'Mode1' 
    rdb.set("mode_type", mode_select)
    #end block
    
    j=0
    while j<5:
        BMS_Param = Perform_OCV(spi, data, table_name)
        time.sleep(5) #Delay of 2 mins
        j+=1

    #Start Charging
    Update_Charging_Parameters(spi,Charging_Voltage_Predefined,Charging_Current_Predefined)
    time.sleep(1)
    Start_Charging(spi)
    #Charging Started
    
    #time.sleep(5) #wait for some time for voltage levels to stabilize (5mins)
    #BMS_Param = Perform_OCV(spi,  data, table_name)

    #Check if voltages have set to Recommended Levels

    #End Block

    #Start Charging Loop

    Curr_Timestamp = datetime.datetime.now().timestamp()
    Current_Reuction_Flag == 0

    while Curr_Timestamp <= Loop_Time: #This will be replaced by (Duration - 3) hrs (72 hr)
        BMS_Param = Read_BMS_Parameters(spi, data, table_name)
        
        #Slpicing List into Components
        Voltage = BMS_Param[0:10]
        print(Voltage)
        Temperature = BMS_Param[10:20]
        print(Temperature)
        SOC = BMS_Param[20:30]
        print(SOC)
        Current = BMS_Param[30:40]
        print(Current)
        Alarm = BMS_Param[40:50]
        
        #Fully Charged? Check Block
        Min_SOC = min(SOC)
        Min_Voltage = min(Voltage)
        if Min_SOC >= 9900 and (Min_Voltage >= Full_Charge_Voltage_L and Min_Voltage <= Full_Charge_Voltage_H):
            break
        else:
            pass
        #End Block

        #Alarm Block
        """for i in Alarm:
            if Alarm[i] != 0:
                #Alert()
                pass
            else:
                pass """   
        #End Block

        #Check Safety Conditions
        Max_Temperature = max(Temperature)
        if Max_Temperature >= High_Temperature_Level and Current_Reuction_Flag == 0:
            Update_Charging_Parameters(spi,Charging_Voltage_Predefined,int(Charging_Current_Predefined/2))
            Current_Reuction_Flag = 1
        elif Max_Temperature >= Critical_Temperature_Level:
            Stop_Charging(spi)
            Critical_Alarm_Flag = 1
            break
        #End Block

        
        print("Loop Done")
        
        #Read Stop flag from redis

        #End Block
        #Add delay for 5mins
        time.sleep(5) #Delay for 5mins
        Curr_Timestamp = datetime.datetime.now().timestamp()

    #Read Stop flag from redis

    #End Block
    
    
    #Check if Critical Condition Occured
    if Critical_Alarm_Flag == 1:
        Emergency_Stop_Routine(spi)
    else:
        count = 0
        Check = [0,0,0,0,0,0,0,0,0,0]
        Prev_Voltage = BMS_Param[0:10]
        Curr_Timestamp = datetime.datetime.now().timestamp()
        while Curr_Timestamp <= Max_Time:
            BMS_Param = Perform_OCV(spi,  data, table_name)
            Voltage = BMS_Param[0:10]
            i=0
            j=0
            for i in Prev_Voltage:
                Low_Lim =  i - 50
                High_Lim = i + 50
                if Voltage[j] >= Low_Lim and Voltage[j] <= High_Lim:
                    #Unchanged voltage
                    Check[j] += 1
                else:
                    #Changed voltage
                    Check[j] = 0
                j+=1
            if min(Check) >= 15:
                #Stop Charging
                Stop_Charging(spi)
                print("Charging Stopped")
                break
            else:
                #Keep Charging
                pass
            Prev_Voltage = [0,0,0,0,0,0,0,0,0,0]
            Prev_Voltage = Voltage.copy()
            count += 1
            

            #Read Stop flag from redis

            #End Block

            time.sleep(5) #Delay for 10 mins
            count += 1
            Curr_Timestamp = datetime.datetime.now().timestamp()
        Stop_Charging(spi)

        #Charging completed now check for Voltage retention and identify weak batteries
        BMS_Param = Perform_OCV(spi,  data, table_name)
        Voltage = BMS_Param[0:10]
        time.sleep(5)#Delay of 15 mins
        BMS_Param = Perform_OCV(spi,  data, table_name)
        Temp = BMS_Param[0:10]
        Fault_Entry = [0,0,0,0,0,0,0,0,0,0]
        i=0
        m=0
        for m in Fault_Entry:
            if Temp[i] <= Fault_Voltage_Level:
                Fault_Entry[i] = 'Maybe Faulty'
            else:
                Fault_Entry[i] = 'Healthy'
            i+=1    
        
        #Add log of Battery Health onto a database
        conn = sqlite3.connect('Fault.db')
        Fault_Table_Create_Query = '''CREATE TABLE IF NOT EXISTS {}(
                                        id integer PRIMARY KEY,
                                        B1 text,B2 text,B3 text,B4 text,B5 text,B6 text,B7 text,B8 text,B9 text,B10 text
                                    )'''.format(table_name)
        conn.execute(Fault_Table_Create_Query)
        Fault_Table_Insert_Query = '''INSERT INTO  {}(B1,B2,B3,B4,B5,B6,B7,B8,B9,B10) VALUES(?,?,?,?,?,?,?,?,?,?)'''.format(table_name)
        conn.execute(Fault_Table_Insert_Query, tuple(Fault_Entry))
        conn.commit()
        conn.close()
        data.close_connection()
        Stop_Charging(spi)
        spi.close()
        # connect to redis
        rdb = redis.Redis(host="127.0.0.1", port=6379, db=0)
        mode_select = 'Mode1' 
        rdb.set("mode_type", mode_select)
    print("Charging Complete")




def main():
    k=0
    while k < 20:
        redis_val = connect_redis().decode()
        #spi = Start_Spi()
        #data = connect_sqlite()
        #table_name = table_name_generator()
        #data.create_table(table_name)
        if redis_val == 'Mode1':
            print("No mode selected")
        elif redis_val == 'Mode2':
            Constant_Current_Charging()
        elif redis_val == 'Mode3':
            Boost_Charging()
        elif redis_val == 'Mode4':
            Float_Charging()
        elif redis_val == 'Mode5':
            Trickle_Charging()
        else:
            pass    
        time.sleep(5)
        k+=1 

if __name__ == "__main__":
   main()
