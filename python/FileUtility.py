#!/usr/bin/env python
# title           : Multipurpose utility for file conversion and file comparison
# description     : csv to ascii char(1), csv to parquet & vice versa
# author          :
# date            :
# version         :1.0
# usage           :FileUtility option1 option2 <input_filename> <output_filename>
# python_version  :3.5.x
# =======================================================================

# Import the modules needed to run the script.
import sys, os
import time
import fastparquet
from fastparquet import write

import pandas as pd
from fastparquet import ParquetFile

SOH = '\x01'

# Main definition - constants
menu_actions = {}


# =======================
#     MENUS FUNCTIONS
# =======================

# Main menu
def main_menu():
    #os.system('clear')
    #print(sys.argv.__len__())
    #print(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
    if sys.argv.__len__() < 5:
        help_message()
        exit()

    exec_menu()
    return


def help_message():
    print("Usage: option1 option2 <input_filename> <output_filename>")
    print("available choices for option1 - ")
    print(" c -> file conversion")
    print(" d -> file comparison")

    print("available choices for option2 -")
    print(" if file conversion is selected as option1 (option1=c)")
    print("  1 -> convert csv to ascii char(1)")
    print("  2 -> convert ascii char(1) to csv")
    print("  3 -> convert csv to parquet")
    print("  4 -> convert parquet to csv")

    print(" if file comparison is selected as option1 (option1=d)")
    print("  1 -> compare parquet files")




# Execute menu
def exec_menu():
    #os.system('clear')
    primarytask = sys.argv[1]
    subtask = sys.argv[2]

    if primarytask == 'c' and (subtask == '1' or subtask == '2' or subtask =='3' or subtask == '4'):
        conversion_menu[subtask]()
    elif primarytask == 'd' and (subtask == '1'):
        comparison_menu[subtask]()
    else:
        help_message()

    return

# convert csv to ascii char(1)
def csv_to_ascii_char1():
    #print("input file "+sys.argv[2])
    #print("ouput file "+sys.argv[3])

    data = pd.read_csv(sys.argv[3])
    data.to_csv(sys.argv[4], sep=SOH, index=False)

    #print("Done")
    #time.sleep(5)

    return

# convert csv to parquet
def csv_to_parquet():
    #print("input file " + sys.argv[2])
    #print("ouput file " + sys.argv[3])
    df = pd.read_csv(sys.argv[3],dtype=str,lineterminator='\n')
    write(sys.argv[4], df, compression='SNAPPY',write_index=False)
    #time.sleep(5)
    return

# convert ascii char(1) to csv
def ascii_char1_to_csv():
    #print("input file "+sys.argv[2])
    #print("ouput file "+sys.argv[3])
    asciidata = pd.read_csv(sys.argv[3], sep=SOH)

    asciidata.to_csv(sys.argv[4], index=False)

    #print("Done")

    return

# convert parquet to csv
def parquet_to_csv():

    pf = ParquetFile(sys.argv[3])
    df2 = pf.to_pandas()
    df2.to_csv(sys.argv[4], index=False)
    return

# merge multiple parquet files
def parquet_merge_to_csv():

    pf = ParquetFile(sys.argv[3])
    df2 = pf.to_pandas()
    df2.to_csv(sys.argv[4], index=False)
    return

# compare parquet files
def compare_parquet_files():
    pf1 = ParquetFile(sys.argv[3])
    pf2 = ParquetFile(sys.argv[4])

    df1 = pf1.to_pandas()
    df2 = pf2.to_pandas()


    #equality = (df1 != df2).any(1)
    #print(equality)
    #result = (equality == False).any()


    if df1.equals(df2):
        print("Parquet files same.")
        return 1
    else:
        print("Parquet files are different.")
        return 0

    return


# Exit program
def exit():
    sys.exit()


# =======================
#    MENUS DEFINITIONS
# =======================

# File conversion menu definition
conversion_menu = {
    'main_menu': main_menu,
    '1': csv_to_ascii_char1,
    '2': ascii_char1_to_csv,
    '3': csv_to_parquet,
    '4': parquet_to_csv
}
# File comparison menu definition
comparison_menu = {
    'main_menu': main_menu,
    '1': compare_parquet_files,

}


# =======================
#      MAIN PROGRAM
# =======================

# Main Program
if __name__ == "__main__":
    # Launch main menu
    main_menu()
