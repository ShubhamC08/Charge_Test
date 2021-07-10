from gpiozero import LED
import spidev
import random
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
    spi.max_speed_hz = 100000
    spi.mode = 0b01
    
    return spi 

#Create Database connection function 
def Create_DB_Connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    
    return conn

#Create New Table in database
def Create_Table(conn, Create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(Create_table_sql)
    except Error as e:
        print(e)    

#Insert data into the table
def Insert_Data(conn, InsertCmd, InsertData):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    """sql = ''' INSERT INTO BatLog(Timestamp,V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,A1,A2,A3,A4,A5,A6,A7,A8,A9,A10)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ''' """
    try:
        cur = conn.cursor()
        cur.execute(InsertCmd, InsertData)
        conn.commit()
    except Error as e:
        print(e)

    return cur.lastrowid

def connect_redis():
    """
    connect ot redis database and gets the value from the database
    :return:
    """
    # connect to redis
    rdb = redis.Redis(host="127.0.0.1", port=6379, db=15)
    # sample a new value random times between 100ms and 2s
    mode_value = rdb.get('mode_type')
    print(str(mode_value))
    return mode_value

def main():
    #Redis Connection Block
    #write_redis("Mode2")
    redis_value = connect_redis().decode()
    #Open SPI connection
    spi = Start_Spi()

    #Database Location
    Database = r"/home/pi/Test_Dummy.db"
    
    #Create a connection to Database
    conn = Create_DB_Connection(Database)
    
    #Create a table in Database with Specific name
    #String Maniplulation Block
    Create_Table_SQL_CMD = """ CREATE TABLE IF NOT EXISTS BatLog (
                                        id integer PRIMARY KEY,
                                        Timestamp text NOT NULL,
                                        V1 real,V2 real,V3 real,V4 real,V5 real,V6 real,V7 real,V8 real,V9 real,V10 real,
                                        T1 real,T2 real,T3 real,T4 real,T5 real,T6 real,T7 real,T8 real,T9 real,T10 real,
                                        C1 real,C2 real,C3 real,C4 real,C5 real,C6 real,C7 real,C8 real,C9 real,C10 real,
                                        I1 real,I2 real,I3 real,I4 real,I5 real,I6 real,I7 real,I8 real,I9 real,I10 real,
                                        A1 real,A2 real,A3 real,A4 real,A5 real,A6 real,A7 real,A8 real,A9 real,A10 real
                                    ); """

    Insert_Data_SQL_CMD = ''' INSERT INTO BatLog(Timestamp,V1,V2,V3,V4,V5,V6,V7,V8,V9,V10,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,A1,A2,A3,A4,A5,A6,A7,A8,A9,A10)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    i = 5
    
    #Create Table command execution
    Create_Table(conn, Create_Table_SQL_CMD)

    """
    if redis_value == "Mode1":
        Algorithms.Constant_Current_Charging(spi, conn)
        print("Mode 1 running algorithms.Constant_Current_Charging()")
    elif redis_value == "Mode2":
        Algorithms.Boost_Charging(spi, conn)
        print("Mode 2 running algorithms.Constant_Boost_Charging()")
    else:
        print("No Mode running!") """

    Constant_Current_Charging(spi, conn,Insert_Data_SQL_CMD)
    
    #Charging Completion Block
     #Sleep process for some time

     #After Charging Testing


    #Stop Charging
    #Close SPI connection
    spi.close()
    #Close connection to database
    conn.close()

def Perform_OCV(spi,conn,Insert_Data_SQL_CMD):
    #OCV Block Start
    OCV_Command =[0x1,0x0,0x0,0xFE]
    spi.writebytes(OCV_Command)
    #Dummy_Read = spi.readbytes(3)
    time.sleep(0.05)
    Read_BMS_Command = [0x2,0x0,0x0,0xFE]
    spi.writebytes(Read_BMS_Command)
    #Dummy_Read = spi.readbytes(3)
    #time.sleep(0.05)
    Dummy_Write = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    Rcvd_Data = spi.xfer(Dummy_Write)
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
    Time = datetime.datetime.now().strftime('%m-%d-%Y_%H.%M.%S')
    BMS_Data.insert(0, Time)
    BMS_Store_Data = tuple(BMS_Data)
    #Store Data onto Database
    Insert_Data(conn, Insert_Data_SQL_CMD, BMS_Store_Data)
    #OCV Block End
    return Temp

def Read_BMS_Parameters(spi,conn,Insert_Data_SQL_CMD):
    #Coulumb_Counting Block Start
    Read_BMS_Command =[0x2,0x0,0x0,0xFE]
    spi.writebytes(Read_BMS_Command)
    #Dummy_Read = spi.readbytes(3)
    #time.sleep(0.05)
    Dummy_Write = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    Rcvd_Data = spi.xfer(Dummy_Write)
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
    Time = datetime.datetime.now().strftime('%m-%d-%Y_%H.%M.%S') 
    BMS_Data.insert(0, Time)
    BMS_Store_Data = tuple(BMS_Data)
    #Store Data onto Database
    Insert_Data(conn, Insert_Data_SQL_CMD, BMS_Store_Data)
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
    Start_Charging_Command = [0x5,0x0,0x0,0xFE]
    spi.writebytes(Start_Charging_Command)
    #D_Read = spi.readbytes(3)

def Stop_Charging(spi):
    Stop_Charging_Command = [0x6,0x0,0x0,0xFE]
    spi.writebytes(Stop_Charging_Command)
    #D_Read = spi.readbytes(3)

def Alert():
    led = LED(17)
    i=0
    while i<3:
        led.on()
        sleep(0.5)
        led.off()
        sleep(0.5)
        i+=1

def Emergency_Stop_Routine(spi):
    Stop_Charging(spi)
    led = OUTPUT_PIN(17)
    while User_Acknowledge_Flag == 0:
        led.on()

def Constant_Current_Charging(spi,conn,Insert_Data_SQL_CMD):
    Charging_Voltage_Predefined = 1300 #130.0V
    Charging_Current_Predefined = 75 #7.5A
    Charging_Voltage_Stopped = 0
    Charging_Current_Stopped = 0
    Optimal_Higher_Charge_Volts = 1275 #12.75V
    Optimal_Lower_Charge_Volts = 1055 #10.55V
    Full_Charge_Voltage = 1375 #13.75V
    High_Temperature_Level = 4500
    Critical_Temperature_Level = 5500
    Exit_Flag = 0
    Critical_Alarm_Flag = 0
    Fault=[0,0,0,0,0,0,0,0,0,0]
    i=0
    while i<5:
        BMS_Param = Perform_OCV(spi,conn,Insert_Data_SQL_CMD)
        time.sleep(5)
        i+=1
    Update_Charging_Parameters(spi,Charging_Voltage_Predefined,Charging_Current_Predefined)
    time.sleep(1)
    Start_Charging(spi)
    time.sleep(5)
    BMS_Param = Perform_OCV(spi,conn,Insert_Data_SQL_CMD)
    #Check if voltages have set to Recommended Levels

    #End Block
    #Start Charging Loop
    j=0
    while j<5:
        BMS_Param = Read_BMS_Parameters(spi,conn,Insert_Data_SQL_CMD)
        #Slpicing List into Components
        Voltage = BMS_Param[0:10]
        Temperature = BMS_Param[10:20]
        SOC = BMS_Param[20:30]
        Current = BMS_Param[30:40]
        Alarm = BMS_Param[40:50]
        #Fully Charged? Check Block
        Min_SOC = min(SOC)
        Min_Voltage = min(Voltage)
        if Min_SOC >= 9900 and Min_Voltage >= Full_Charge_Voltage:
            Exit_Flag = 1
        else:
            Exit_Flag = 0
        #End Block
        #Alarm Block
        for i in Alarm:
            if Alarm[i] != 0:
                Alert()
            else:
                pass    
        #End Block
        #Check Safety Conditions
        Max_Temperature = max(Temperature)
        if Max_Temperature >= High_Temperature_Level:
            Update_Charging_Parameters(spi,Charging_Voltage_Predefined,(Charging_Current_Predefined/2))
        elif Max_Temperature >= Critical_Temperature_Level:
            Stop_Charging(spi)
            Critical_Alarm_Flag = 1
            break
        #End Block
        #Timestamp Update Block

        #End Block
        #Add delay for 5mins
        print("Loop Done")
        time.sleep(5)
        j+=1
    #Stopped Charging
    if Critical_Alarm_Flag == 1:
        Emergency_Stop_Routine(spi,conn)
    else:
        #Update_Charging_Parameters(spi,Charging_Voltage_Stopped,Charging_Current_Stopped)
        Stop_Charging(spi)
        time.sleep(10)
        print("Charging Stopped")
        j=0
        while j<3:
            #Perform OCV
            BMS_Param = Perform_OCV(spi,conn,Insert_Data_SQL_CMD)
            #Slpicing List into Components
            Voltage = BMS_Param[0:10]
            Temperature = BMS_Param[10:20]
            SOC = BMS_Param[20:30]
            Current = BMS_Param[30:40]
            Alarm = BMS_Param[40:50]
            #Check Block
            for i in Voltage:
                if Voltage[i] <= Optimal_Lower_Charge_Volts and Voltage[i] >= Optimal_Higher_Charge_Volts:
                    Fault[i] = Fault[i] + 1
                else:
                    pass
            #End Check Block
            #Add Delay for 1 hour
            time.sleep(20)
            j+=1
    print("All Done")

def Boost_Charging(spi,conn):
    Charging_Voltage_Predefined = 1300 #130.0V
    Charging_Current_Predefined = 75 #7.5A
    Charging_Voltage_Stopped = 0
    Charging_Current_Stopped = 0
    Optimal_Higher_Charge_Volts = 1275 #12.75V
    Optimal_Lower_Charge_Volts = 1055 #10.55V
    Full_Charge_Voltage = 1375 #13.75V
    Exit_Flag = 0
    Critical_Alarm_Flag = 0
    Fault=[0,0,0,0,0,0,0,0,0,0]
    i=0
    while i<5:
        BMS_Param = Perform_OCV(spi,conn)
        time.sleep(120)
    Update_Charging_Parameters(spi,Charging_Voltage_Predefined,Charging_Current_Predefined)
    time.sleep(10)
    Start_Charging(spi)
    time.sleep(300)
    BMS_Param = Perform_OCV(spi,conn)
    #Check if voltages have set to Recommended Levels

    #End Block
    #Start Charging Loop
    while Exit_Flag == 0: #Add time here too
        BMS_Param = Read_BMS_Parameters(spi,conn)
        #Slpicing List into Components
        Voltage = BMS_Param[0:10]
        Temperature = BMS_Param[10:20]
        SOC = BMS_Param[20:30]
        Current = BMS_Param[30:40]
        Alarm = BMS_Param[40:50]
        #Fully Charged? Check Block
        Min_SOC = min(SOC)
        Min_Voltage = min(Voltage)
        if Min_SOC >= 9900 and Min_Voltage >= Full_Charge_Voltage:
            Exit_Flag = 1
        else:
            Exit_Flag = 0
        #End Block
        #Alarm Block
        for i in Alarm:
            if Alarm[i] != 0:
                Alert()
            else:
                pass    
        #End Block
        #Check Safety Conditions
        Max_Temperature = max(Temperature)
        if Max_Temperature >= High_Temperature_Level:
            Update_Charging_Parameters(spi,Charging_Voltage_Predefined,(Charging_Current_Predefined/2))
        elif Max_Temperature >= Critical_Temperature_Level:
            Stop_Charging(spi)
            Critical_Alarm_Flag = 1
            break
        #End Block
        #Timestamp Update Block

        #End Block
        #Add delay for 5mins
    
        time.sleep(300)
    #Stopped Charging
    if Critical_Alarm_Flag == 1:
        Emergency_Stop_Routine(spi,conn)
    else:
        #Update_Charging_Parameters(spi,Charging_Voltage_Stopped,Charging_Current_Stopped)
        Stop_Charging(spi)
        time.sleep(600)
        i=0
        while i<3:
            #Perform OCV
            BMS_Param = Perform_OCV(spi,conn)
            #Slpicing List into Components
            Voltage = BMS_Param[0:10]
            Temperature = BMS_Param[10:20]
            SOC = BMS_Param[20:30]
            Current = BMS_Param[30:40]
            Alarm = BMS_Param[40:50]
            #Check Block
            for i in Voltage:
                if Voltage[i] <= Optimal_Lower_Charge_Volts and Voltage[i] >= Optimal_Higher_Charge_Volts:
                    Fault[i] = Fault[i] + 1
                else:
                    pass
            #End Check Block
            #Add Delay for 1 hour   
            time.sleep(3600)


if __name__ == '__main__':
    main()


