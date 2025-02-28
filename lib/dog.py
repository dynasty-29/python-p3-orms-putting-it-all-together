import sqlite3

# Set up the database connection and cursor at the module level
CONN = sqlite3.connect('dogs.db')
CURSOR = CONN.cursor()

class Dog:
    def __init__(self, name, breed, id=None):
        self.id = id
        self.name = name
        self.breed = breed
    
    @classmethod
    def create_table(cls):
        """Create the dogs table if it doesn't exist"""
        sql = """
            CREATE TABLE IF NOT EXISTS dogs
                (id INTEGER PRIMARY KEY,
                name TEXT,
                breed TEXT)
        """
        CURSOR.execute(sql)
        CONN.commit()
    
    @classmethod
    def drop_table(cls):
        """Drop the dogs table if it exists"""
        CURSOR.execute("DROP TABLE IF EXISTS dogs")
        CONN.commit()
    
    def save(self):
        """Save the dog to the database"""
        # If dog doesn't have an ID, insert as new record
        if self.id is None:
            sql = """
                INSERT INTO dogs (name, breed)
                VALUES (?, ?)
            """
            CURSOR.execute(sql, (self.name, self.breed))
            CONN.commit()
            self.id = CURSOR.lastrowid
        # Otherwise, update existing record
        else:
            sql = """
                UPDATE dogs 
                SET name = ?, breed = ? 
                WHERE id = ?
            """
            CURSOR.execute(sql, (self.name, self.breed, self.id))
            CONN.commit()
        
        return self
    
    @classmethod
    def create(cls, name, breed):
        """Create a new dog in the database"""
        dog = cls(name, breed)
        dog.save()
        return dog
    
    @classmethod
    def new_from_db(cls, row):
        """Create a new Dog object from database row"""
        id, name, breed = row
        return cls(name, breed, id)
    
    @classmethod
    def get_all(cls):
        """Get all dogs from the database"""
        sql = "SELECT * FROM dogs"
        rows = CURSOR.execute(sql).fetchall()
        
        # Convert rows to Dog objects
        return [cls.new_from_db(row) for row in rows]
    
    @classmethod
    def find_by_name(cls, name):
        """Find a dog by name"""
        sql = """
            SELECT * FROM dogs
            WHERE name = ?
            LIMIT 1
        """
        row = CURSOR.execute(sql, (name,)).fetchone()
        
        if row:
            return cls.new_from_db(row)
        return None
    
    @classmethod
    def find_by_id(cls, id):
        """Find a dog by id"""
        sql = """
            SELECT * FROM dogs
            WHERE id = ?
            LIMIT 1
        """
        row = CURSOR.execute(sql, (id,)).fetchone()
        
        if row:
            return cls.new_from_db(row)
        return None
    
    @classmethod
    def find_or_create_by(cls, name, breed):
        """Find a dog by name and breed or create if not found"""
        sql = """
            SELECT * FROM dogs
            WHERE name = ? AND breed = ?
            LIMIT 1
        """
        row = CURSOR.execute(sql, (name, breed)).fetchone()
        
        if row:
            return cls.new_from_db(row)
        else:
            return cls.create(name, breed)
    
    def update(self):
        """Update the dog in the database"""
        sql = """
            UPDATE dogs
            SET name = ?, breed = ?
            WHERE id = ?
        """
        CURSOR.execute(sql, (self.name, self.breed, self.id))
        CONN.commit()
        
        return self