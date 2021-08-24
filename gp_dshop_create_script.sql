create table aisles
(
    aisle_id integer not null,
    aisle    varchar(127)
);

alter table aisles
    owner to pguser;

create table clients
(
    id               integer not null
        constraint clients_pk
            primary key,
    fullname         varchar(63),
    location_area_id integer
);

alter table clients
    owner to pguser;

create table departments
(
    department_id integer not null,
    department    varchar(127)
);

alter table departments
    owner to pguser;

create table location_areas
(
    area_id integer not null,
    area    varchar(64)
);

alter table location_areas
    owner to pguser;

create table products
(
    product_id    integer not null
        constraint products_pk
            primary key,
    product_name  varchar(255),
    aisle_id      integer,
    department_id integer
);

alter table products
    owner to pguser;


create table store_types
(
    store_type_id integer not null,
    type          varchar(64)
);

alter table store_types
    owner to pguser;

create table stores
(
    store_id         integer not null
        constraint stores_pk
            primary key,
    location_area_id smallint,
    store_type_id    smallint
);

alter table stores
    owner to pguser;

create table orders
(
    product_id integer
        constraint orders_products_product_id_fk
            references products,
    client_id  integer
        constraint orders_clients_id_fk
            references clients,
    store_id   integer
        constraint orders_stores_store_id_fk
            references stores,
    quantity   integer,
    order_date date
);

alter table orders
    owner to pguser;

create table out_of_stock
(
    product_id         integer not null
        constraint out_of_stock_products_product_id_fk
            references products,
    store_id integer not null
        constraint out_of_stock_stores_store_id_fk
            references stores,
    oos_date date
);

alter table out_of_stock
    owner to pguser;

