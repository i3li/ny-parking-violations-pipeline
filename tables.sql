-- >>> Staging Tables >>> --
CREATE TABLE stage_state (
  record_type VARCHAR,
  state_code VARCHAR,
  description VARCHAR
)

CREATE TABLE stage_violation_code (
  code INT,
  description VARCHAR,
  manhattan_amount FLOAT,
  others_amount FLOAT
)

CREATE TABLE stage_vehicle_body_type (
  code VARCHAR,
  type VARCHAR
)

CREATE TABLE stage_vehicle_plate_type (
  code VARCHAR,
  type VARCHAR
)

CREATE TABLE stage_vehicle_color (
  code VARCHAR,
  color VARCHAR
)

CREATE TABLE stage_violation (
  summons_number VARCHAR,
  plate_id VARCHAR,
  registration_state VARCHAR,
  plate_type VARCHAR,
  issue_date DATE,
  violation_code INT,
  vehicle_body_type VARCHAR,
  vehicle_make VARCHAR,
  issuing_agency VARCHAR,
  street_code1 INT,
  street_code2 INT,
  street_code3 INT,
  vehicle_expiration_date DATE,
  violation_location VARCHAR,
  violation_precinct INT,
  issuer_precinct INT,
  issuer_code INT,
  issuer_command VARCHAR,
  issuer_squad VARCHAR,
  violation_time VARCHAR,
  time_first_observed VARCHAR,
  violation_county VARCHAR,
  violation_in_front_of_or_opposite VARCHAR,
  house_number VARCHAR,
  street_name VARCHAR,
  intersecting_street VARCHAR,
  date_first_observed VARCHAR,
  law_section VARCHAR,
  sub_division VARCHAR,
  violation_legal_code VARCHAR,
  days_parking_in_effect VARCHAR,
  from_hours_in_effect VARCHAR,
  to_hours_in_effect VARCHAR,
  vehicle_color VARCHAR,
  unregistered_vehicle VARCHAR,
  vehicle_year INT,
  meter_number VARCHAR,
  feet_from_curb INT,
  violation_post_code VARCHAR,
  violation_description VARCHAR,
  no_Standing_or_stopping_violation VARCHAR,
  hydrant_violation VARCHAR,
  double_parking_violation VARCHAR
)

CREATE TABLE stage_county (
  code VARCHAR,
  county VARCHAR
)

CREATE TABLE stage_issuing_agency (
  code VARCHAR,
  agency VARCHAR
)

-- <<< Staging Tables <<< --
