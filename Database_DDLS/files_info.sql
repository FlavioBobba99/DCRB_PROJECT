CREATE TABLE files_info (
            id INT NOT NULL AUTO_INCREMENT, 
            name VARCHAR(255) NOT NULL, 
            path VARCHAR(255),
            type VARCHAR(255),
            size BIGINT,
            location VARCHAR(512), 
            PRIMARY KEY(id)
        )