CREATE TABLE IF NOT EXISTS pets_linear (
    id SERIAL PRIMARY KEY,
    pet_name VARCHAR(100),
    species VARCHAR(50),
    birth_year INTEGER,
    photo_url TEXT,
    fav_food VARCHAR(100),
    food_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pet_name ON pets_linear(pet_name);
CREATE INDEX IF NOT EXISTS idx_species ON pets_linear(species);
