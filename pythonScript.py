import pandas as pd
import sqlite3
from collections import defaultdict

# Read the spreadsheets into DataFrames
spreadsheet0 = pd.read_excel('shipping_data_0.csv')
spreadsheet1 = pd.read_excel('shipping_data_1.csv')
spreadsheet2 = pd.read_excel('shipping_data_2.csv')

# Create a connection to the SQLite database
conn = sqlite3.connect('shipment_database.db')
cur = conn.cursor()

# Create tables based on the provided ERD
cur.executescript('''
    CREATE TABLE IF NOT EXISTS Product (
        ProductID INTEGER PRIMARY KEY,
        Name TEXT,
        ProductType TEXT,
        ManufacturerID INTEGER
    );

    CREATE TABLE IF NOT EXISTS PetFood (
        ProductID INTEGER PRIMARY KEY,
        Weight REAL,
        Flavor TEXT,
        TargetHealthCondition TEXT
    );

    CREATE TABLE IF NOT EXISTS PetToy (
        ProductID INTEGER PRIMARY KEY,
        Material TEXT,
        Durability TEXT
    );

    CREATE TABLE IF NOT EXISTS PetApparel (
        ProductID INTEGER PRIMARY KEY,
        Color TEXT,
        Size TEXT,
        CareInstructions TEXT
    );

    CREATE TABLE IF NOT EXISTS Animal (
        AnimalID INTEGER PRIMARY KEY,
        AnimalType TEXT
    );

    CREATE TABLE IF NOT EXISTS Manufacturer (
        ManufacturerID INTEGER PRIMARY KEY,
        Name TEXT
    );

    CREATE TABLE IF NOT EXISTS Customer (
        CustomerID INTEGER PRIMARY KEY,
        Name TEXT,
        Email TEXT
    );

    CREATE TABLE IF NOT EXISTS Transaction (
        TransactionID INTEGER PRIMARY KEY,
        CustomerID INTEGER,
        Date TEXT,
        FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
    );

    CREATE TABLE IF NOT EXISTS TransactionProduct (
        TransactionID INTEGER,
        ProductID INTEGER,
        Quantity INTEGER,
        PRIMARY KEY (TransactionID, ProductID),
        FOREIGN KEY (TransactionID) REFERENCES Transaction(TransactionID),
        FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
    );

    CREATE TABLE IF NOT EXISTS WalmartLocation (
        LocationID INTEGER PRIMARY KEY,
        Name TEXT,
        ZipCode TEXT
    );

    CREATE TABLE IF NOT EXISTS Shipment (
        ShipmentID INTEGER PRIMARY KEY,
        OriginLocationID INTEGER,
        DestinationLocationID INTEGER,
        Date TEXT,
        FOREIGN KEY (OriginLocationID) REFERENCES WalmartLocation(LocationID),
        FOREIGN KEY (DestinationLocationID) REFERENCES WalmartLocation(LocationID)
    );

    CREATE TABLE IF NOT EXISTS ShipmentProduct (
        ShipmentID INTEGER,
        ProductID INTEGER,
        Quantity INTEGER,
        PRIMARY KEY (ShipmentID, ProductID),
        FOREIGN KEY (ShipmentID) REFERENCES Shipment(ShipmentID),
        FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
    );

    CREATE TABLE IF NOT EXISTS ProductAnimal (
        ProductID INTEGER,
        AnimalID INTEGER,
        PRIMARY KEY (ProductID, AnimalID),
        FOREIGN KEY (ProductID) REFERENCES Product(ProductID),
        FOREIGN KEY (AnimalID) REFERENCES Animal(AnimalID)
    );
''')

# Insert data from spreadsheet 0 into the database
for index, row in spreadsheet0.iterrows():
    product_type = row['ProductType']
    if product_type == 'PetFood':
        cur.execute('INSERT INTO Product (Name, ProductType, ManufacturerID) VALUES (?, ?, ?)', 
                    (row['Name'], product_type, row['ManufacturerID']))
        product_id = cur.lastrowid
        cur.execute('INSERT INTO PetFood (ProductID, Weight, Flavor, TargetHealthCondition) VALUES (?, ?, ?, ?)', 
                    (product_id, row['Weight'], row['Flavor'], row['TargetHealthCondition']))
    elif product_type == 'PetToy':
        cur.execute('INSERT INTO Product (Name, ProductType, ManufacturerID) VALUES (?, ?, ?)', 
                    (row['Name'], product_type, row['ManufacturerID']))
        product_id = cur.lastrowid
        cur.execute('INSERT INTO PetToy (ProductID, Material, Durability) VALUES (?, ?, ?)', 
                    (product_id, row['Material'], row['Durability']))
    elif product_type == 'PetApparel':
        cur.execute('INSERT INTO Product (Name, ProductType, ManufacturerID) VALUES (?, ?, ?)', 
                    (row['Name'], product_type, row['ManufacturerID']))
        product_id = cur.lastrowid
        cur.execute('INSERT INTO PetApparel (ProductID, Color, Size, CareInstructions) VALUES (?, ?, ?, ?)', 
                    (product_id, row['Color'], row['Size'], row['CareInstructions']))

# Insert data from spreadsheet 2 into the WalmartLocation table
location_map = {}
for index, row in spreadsheet2.iterrows():
    cur.execute('INSERT INTO WalmartLocation (Name, ZipCode) VALUES (?, ?)', 
                (row['LocationName'], row['ZipCode']))
    location_map[row['LocationName']] = cur.lastrowid

# Insert data from spreadsheet 1 into the database
shipment_map = defaultdict(list)
for index, row in spreadsheet1.iterrows():
    shipment_map[row['ShippingIdentifier']].append(row)

for shipment_id, rows in shipment_map.items():
    origin_location_id = location_map[rows[0]['Origin']]
    destination_location_id = location_map[rows[0]['Destination']]
    date = rows[0]['ShipmentDate']
    cur.execute('INSERT INTO Shipment (ShipmentID, OriginLocationID, DestinationLocationID, Date) VALUES (?, ?, ?, ?)', 
                (shipment_id, origin_location_id, destination_location_id, date))
    
    for row in rows:
        product_type = row['ProductType']
        if product_type == 'PetFood':
            cur.execute('INSERT INTO Product (Name, ProductType, ManufacturerID) VALUES (?, ?, ?)', 
                        (row['Name'], product_type, row['ManufacturerID']))
            product_id = cur.lastrowid
            cur.execute('INSERT INTO PetFood (ProductID, Weight, Flavor, TargetHealthCondition) VALUES (?, ?, ?, ?)', 
                        (product_id, row['Weight'], row['Flavor'], row['TargetHealthCondition']))
        elif product_type == 'PetToy':
            cur.execute('INSERT INTO Product (Name, ProductType, ManufacturerID) VALUES (?, ?, ?)', 
                        (row['Name'], product_type, row['ManufacturerID']))
            product_id = cur.lastrowid
            cur.execute('INSERT INTO PetToy (ProductID, Material, Durability) VALUES (?, ?, ?)', 
                        (product_id, row['Material'], row['Durability']))
        elif product_type == 'PetApparel':
            cur.execute('INSERT INTO Product (Name, ProductType, ManufacturerID) VALUES (?, ?, ?)', 
                        (row['Name'], product_type, row['ManufacturerID']))
            product_id = cur.lastrowid
            cur.execute('INSERT INTO PetApparel (ProductID, Color, Size, CareInstructions) VALUES (?, ?, ?, ?)', 
                        (product_id, row['Color'], row['Size'], row['CareInstructions']))

        cur.execute('INSERT INTO ShipmentProduct (ShipmentID, ProductID, Quantity) VALUES (?, ?, ?)', 
                    (shipment_id, product_id, row['Quantity']))

# Commit the transaction and close the connection
conn.commit()
conn.close()
