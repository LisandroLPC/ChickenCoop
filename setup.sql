-- ══════════════════════════════════════════════════════════════
-- THE CHICKEN COOP — setup.sql (v1, noviembre 2026)
-- Base nueva completa: tablas + seguridad. Correr UNA vez en
-- Supabase > SQL Editor > New query > pegar todo > Run.
-- Es seguro correrlo más de una vez: no borra ni duplica nada.
-- ══════════════════════════════════════════════════════════════
begin;

-- USUARIOS
create table if not exists usuarios (
  id text primary key,
  nombre text not null,
  pin text not null,
  rol text not null default 'empleado',
  activo boolean default true,
  created_at timestamptz default now()
);

-- GRUPOS DE STOCK
create table if not exists stock_groups (
  id text primary key,
  name text not null,
  unit text default 'kg',
  stock_qty numeric default 0,
  tipo text default 'venta',
  cost_unit numeric default 0,
  updated_at timestamptz default now()
);

-- VARIANTES DE VENTA
create table if not exists stock_variants (
  id text primary key,
  group_id text references stock_groups(id) on delete cascade,
  name text not null,
  qty_per_unit numeric not null default 1,
  price numeric default 0,
  updated_at timestamptz default now()
);

-- CLIENTES (contado y cuenta corriente)
create table if not exists clientes_cc (
  id text primary key,
  nombre text not null,
  telefono text,
  time text,
  created_at timestamptz default now()
);

-- VENTAS
create table if not exists ventas (
  id text primary key,
  day date not null,
  variant_id text,
  group_id text,
  qty numeric not null,
  stock_used numeric not null,
  price_unit numeric not null,
  descuento_pct numeric default 0,
  total numeric not null,
  pago text not null,
  time text,
  created_at timestamptz default now(),
  ticket_id text,
  pago_ef numeric default 0,
  pago_tr numeric default 0,
  costo_unit_venta numeric default 0,
  usuario text,
  cliente_cc_id text,
  cc_pagado boolean default false
);

-- MOVIMIENTOS DE CAJA (ingresos/egresos que no son ventas, cobros de Cta.Cte.)
create table if not exists caja_movimientos (
  id text primary key,
  day date not null,
  tipo text not null,
  descripcion text not null,
  metodo text not null,
  monto numeric not null,
  time text,
  created_at timestamptz default now(),
  usuario text,
  cliente_cc_id text
);

-- COMPRAS / FACTURAS
create table if not exists compras (
  id text primary key,
  day date not null,
  proveedor text not null,
  nro_factura text,
  total numeric not null,
  pago_efectivo numeric default 0,
  pago_transferencia numeric default 0,
  note text,
  time text,
  created_at timestamptz default now(),
  grupos_propagados text default '[]',
  gasto_id text,
  usuario text
);

create table if not exists compras_items (
  id text primary key,
  compra_id text references compras(id) on delete cascade,
  descripcion text not null,
  tipo_destino text not null,
  ref_id text,
  qty_compra numeric not null,
  unit_compra text,
  qty_real numeric,
  unit_real text,
  precio_total numeric not null,
  cost_unit_calculado numeric,
  created_at timestamptz default now(),
  usado boolean default false,
  upd_stock boolean default true,
  upd_cost boolean default true
);

-- PRODUCCIÓN — CORTE
create table if not exists cortes (
  id text primary key,
  day date not null,
  nombre text not null,
  note text,
  time text,
  created_at timestamptz default now(),
  origen_compra_item_id text,
  usuario text
);

create table if not exists cortes_items (
  id text primary key,
  corte_id text references cortes(id) on delete cascade,
  group_id text,
  nombre text not null,
  qty numeric not null,
  unit text,
  created_at timestamptz default now(),
  cost_unit_aplicado numeric default 0
);

-- PRODUCCIÓN — ELABORACIÓN
create table if not exists elaboraciones (
  id text primary key,
  day date not null,
  nombre text not null,
  output_group_id text,
  output_qty numeric default 0,
  costo_total_info numeric default 0,
  note text,
  time text,
  created_at timestamptz default now(),
  usuario text
);

create table if not exists elaboraciones_items (
  id text primary key,
  elaboracion_id text references elaboraciones(id) on delete cascade,
  tipo text not null,
  ref_id text not null,
  nombre text not null,
  qty numeric not null,
  unit text,
  costo_unit numeric default 0,
  costo_subtotal numeric default 0,
  created_at timestamptz default now()
);

-- GASTOS (operativos de caja, del local y los vinculados a compras)
create table if not exists gastos (
  id text primary key,
  day date not null,
  descripcion text not null,
  cat text not null,
  amount numeric not null,
  time text,
  created_at timestamptz default now(),
  metodo text default 'efectivo',
  auto boolean default false,
  pago_efectivo numeric default 0,
  pago_transferencia numeric default 0,
  usuario text
);

-- INSUMOS
create table if not exists insumos (
  id text primary key,
  name text not null,
  unit text not null,
  cost_unit numeric default 0,
  updated_at timestamptz default now(),
  stock_qty numeric default 0
);

-- CIERRES DE CAJA
create table if not exists cierres (
  id text primary key,
  day date not null unique,
  total_contado numeric not null,
  retiro numeric default 0,
  saldo_siguiente numeric default 0,
  fondo_inicial_manual numeric,
  detalle jsonb,
  time text,
  created_at timestamptz default now(),
  fondo_digital_manual numeric,
  saldo_digital_real numeric,
  saldo_digital_siguiente numeric
);

-- ══════════════════════════════════════════════════════════════
-- MIGRACIÓN OCTUBRE 2026 (v7) — lo nuevo
-- ══════════════════════════════════════════════════════════════

-- Gastos del local (alquiler, luz, arreglos...): cuentan en el resultado, NO en el cierre de caja
alter table gastos add column if not exists fuera_caja boolean default false;

-- Cliente en cualquier venta (contado o Cta.Cte.)
alter table ventas add column if not exists cliente_id text;

-- Fecha del último costo cargado a mano (el costo promedio arranca a contar desde ahí)
alter table stock_groups add column if not exists cost_manual_at timestamptz;

-- Subproductos (carcaza, menudos): costo $0, el costo del cajón lo cargan los cortes principales
alter table stock_groups add column if not exists subproducto boolean default false;

-- Precios especiales por cliente
create table if not exists cliente_precios (
  id text primary key,
  cliente_id text not null references clientes_cc(id) on delete cascade,
  variant_id text not null references stock_variants(id) on delete cascade,
  price numeric not null,
  updated_at timestamptz default now()
);
create unique index if not exists cliente_precios_cliente_variante on cliente_precios(cliente_id, variant_id);


-- RETIROS EN MERCADERÍA (consumo personal del dueño)
create table if not exists retiros_stock (
  id text primary key,
  day date not null,
  group_id text references stock_groups(id) on delete set null,
  variant_id text references stock_variants(id) on delete set null,
  nombre text not null,
  qty numeric not null,            -- cantidad en la unidad de la variante
  stock_used numeric not null,     -- lo que bajó del stock (kg/unidad del grupo)
  unit text default 'kg',
  costo_unit numeric default 0,    -- costo promedio del grupo en el momento del retiro
  costo_total numeric default 0,
  valor_venta numeric default 0,   -- lo que hubiera valido vendido a precio de lista
  motivo text default 'consumo_personal',
  note text,
  usuario text,
  time text,
  created_at timestamptz default now()
);
create index if not exists retiros_stock_day on retiros_stock(day);


-- Usuarios internos de la app (PIN). Solo si la tabla está vacía.
-- CAMBIÁ ESTOS PIN APENAS ENTRES (Config > Usuarios).
insert into usuarios (id, nombre, pin, rol)
select * from (values ('usr_dueno','Dueño','1234','dueno'),('usr_empleado','Empleado','0000','empleado')) v(id,nombre,pin,rol)
where not exists (select 1 from usuarios);

-- ══ SEGURIDAD ══════════════════════════════════════════════════
-- Solo un dispositivo con sesión iniciada (usuario de Authentication)
-- puede leer y escribir. Sin sesión, la base no muestra nada.
grant usage on schema public to authenticated;
do $$
declare t text;
begin
  foreach t in array array['usuarios','stock_groups','stock_variants','insumos','clientes_cc','cliente_precios',
    'ventas','caja_movimientos','compras','compras_items','cortes','cortes_items','elaboraciones',
    'elaboraciones_items','gastos','cierres','retiros_stock']
  loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists solo_local on public.%I', t);
    execute format('create policy solo_local on public.%I for all to authenticated using (true) with check (true)', t);
    execute format('revoke all on public.%I from anon', t);
    execute format('grant select, insert, update, delete on public.%I to authenticated', t);
  end loop;
end $$;

-- Confirma el usuario de acceso por si se creó sin tildar "Auto confirm user"
update auth.users set email_confirmed_at = now() where email_confirmed_at is null;

commit;

-- ══ CONTROL ════════════════════════════════════════════════════
-- Tiene que dar 17 filas, todas con rls = true, anon_puede = false y logueado_puede = true
select c.relname tabla, c.relrowsecurity rls,
  has_table_privilege('anon', c.oid, 'select') anon_puede,
  has_table_privilege('authenticated', c.oid, 'insert') logueado_puede
from pg_class c join pg_namespace n on n.oid=c.relnamespace
where n.nspname='public' and c.relkind='r' order by 1;
