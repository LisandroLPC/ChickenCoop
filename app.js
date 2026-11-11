/* THE CHICKEN COOP v1.0 — basado en LPC v9.0 */
const SB='https://vmsjwvudqxfadrcasexu.supabase.co';
const SK='sb_publishable_iR1iNeJ1U1tkTMzYgfpelA_ESUgNOTs';
const LC={g(k){try{return JSON.parse(localStorage.getItem('tcc1_'+k))||null}catch{return null}},s(k,v){localStorage.setItem('tcc1_'+k,JSON.stringify(v))}};

/* ══ ACCESO DEL DISPOSITIVO (Supabase Auth) ═══════════════════════
   Cada dispositivo inicia sesión UNA vez con la cuenta del local y queda vinculado.
   El token dura 1 hora y se renueva solo. Sin internet se sigue trabajando:
   todo queda en la cola y sube cuando vuelve la conexión.
   Sin sesión válida la base no deja leer ni escribir nada (RLS).          */
let authNecesaria=false,refrescando=null;
function auGet(){return LC.g('auth');}
function auGuardar(j){LC.s('auth',{access_token:j.access_token,refresh_token:j.refresh_token,expires_at:Date.now()+((j.expires_in||3600)*1000),email:j.user?.email||auGet()?.email||''});authNecesaria=false;}
function auRefrescar(){
  if(refrescando)return refrescando;
  refrescando=(async()=>{
    const a=auGet();if(!a?.refresh_token){authNecesaria=true;return false;}
    let r;
    try{r=await fetch(SB+'/auth/v1/token?grant_type=refresh_token',{method:'POST',headers:{apikey:SK,'Content-Type':'application/json'},body:JSON.stringify({refresh_token:a.refresh_token})});}
    catch(e){return false;} // sin conexión: se reintenta en el próximo pedido
    if(r.ok){auGuardar(await r.json());return true;}
    const b=auGet();if(b&&b.refresh_token!==a.refresh_token)return true; // otra pestaña lo renovó recién
    if(r.status>=400&&r.status<500)authNecesaria=true; // sesión vencida o revocada: hay que volver a ingresar
    return false;
  })().finally(()=>{refrescando=null;});
  return refrescando;
}
async function auToken(){
  const a=auGet();if(!a)return null;
  if(a.expires_at-Date.now()<120000)await auRefrescar();
  return auGet()?.access_token||null;
}
async function sbH(extra){const t=await auToken();return{apikey:SK,'Content-Type':'application/json','Prefer':'return=minimal',...(t?{Authorization:'Bearer '+t}:{}),...(extra||{})};}
// Error de red (reintentable) vs. error de la base (permanente: dato o columna inválida)
function sbErr(msg,permanente){const e=new Error(msg);e.permanente=permanente;return e;}
async function sbFetch(url,opt,reintento){
  let r;
  try{r=await fetch(url,opt);}catch(e){throw sbErr('Sin conexión',false);}
  const sinPermiso=r.status===401||r.status===403;
  if(sinPermiso&&!reintento&&auGet()&&await auRefrescar())
    return sbFetch(url,{...opt,headers:{...opt.headers,Authorization:'Bearer '+auGet().access_token}},true);
  if(!r.ok){
    const txt=await r.text().catch(()=>'');
    if(sinPermiso)authNecesaria=true;
    // sin permiso NO es error del dato: queda en la cola hasta que el dispositivo vuelva a ingresar
    throw sbErr(txt||('HTTP '+r.status),r.status>=400&&r.status<500&&!sinPermiso&&r.status!==408&&r.status!==429);
  }
  return r;
}
// Lee TODA la tabla, de a 1000 filas (Supabase no devuelve más de 1000 por consulta)
const SB_PAGINA=1000;
async function sbQ(t,q=''){
  let filas=[],desde=0;
  while(true){
    const r=await sbFetch(SB+'/rest/v1/'+t+'?'+q,{headers:await sbH({'Range-Unit':'items','Range':desde+'-'+(desde+SB_PAGINA-1)})});
    const lote=await r.json();
    filas=filas.concat(lote);
    if(lote.length<SB_PAGINA)return filas;
    desde+=SB_PAGINA;
  }
}
async function sbUpRaw(t,d){const arr=Array.isArray(d)?d:[d];await sbFetch(SB+'/rest/v1/'+t,{method:'POST',headers:await sbH({'Prefer':'resolution=merge-duplicates,return=minimal'}),body:JSON.stringify(arr)});}
async function sbDelRaw(t,id){await sbFetch(SB+'/rest/v1/'+t+'?id=eq.'+encodeURIComponent(id),{method:'DELETE',headers:await sbH()});}


/* ══ COLA DE SINCRONIZACIÓN ═══════════════════════════════════════
   Todo lo que se guarda pasa por acá: primero queda en el celular (cola),
   y se sube a Supabase en orden. Si se corta internet, se reintenta solo
   cada 20 segundos y al volver la conexión. Nada se pierde al recargar.
   Si la base rechaza un registro (dato inválido), queda en "con error"
   para reintentarlo o revisarlo, sin frenar al resto.                  */
let OB=LC.g('outbox')||[];       // pendientes de subir, en orden
let OBerr=LC.g('outbox_err')||[]; // rechazados por la base
let flushing=false;
function obSave(){LC.s('outbox',OB);LC.s('outbox_err',OBerr);}
function sbUp(t,d){OB.push({k:'up',t,d:JSON.parse(JSON.stringify(d)),ts:Date.now()});obSave();flush();}
function sbDel(t,id){OB.push({k:'del',t,id,ts:Date.now()});obSave();flush();}
async function flush(){
  if(flushing)return;
  if(!OB.length){updSync();return;}
  flushing=true;sync('busy','guardando...');
  while(OB.length){
    const op=OB[0];
    try{
      if(op.k==='up')await sbUpRaw(op.t,op.d);else await sbDelRaw(op.t,op.id);
      OB.shift();obSave();
    }catch(e){
      if(e.permanente){OBerr.push({...op,err:String(e.message||e).slice(0,400)});OB.shift();obSave();continue;}
      break; // sin conexión: queda en la cola y se reintenta después
    }
  }
  flushing=false;updSync();
  if((OBerr.length&&tab==='caja')||authNecesaria)render();
}
function updSync(){
  if(authNecesaria){sync('err','sin sesión');return;}
  if(OBerr.length)sync('err',OBerr.length+' con error');
  else if(OB.length)sync('busy',OB.length+' sin subir');
  else sync('ok','sincronizado');
}
function reintentarErrores(){if(!OBerr.length)return;OB=[...OBerr.map(({err,...op})=>op),...OB];OBerr=[];obSave();render();flush();}
function descartarErrores(){
  if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede descartar registros.');
  if(!confirm(`¿Descartar ${OBerr.length} registro(s) que la base rechazó? No se van a subir nunca.`))return;
  OBerr=[];obSave();render();updSync();
}
function verErrores(){alert(OBerr.map((o,i)=>`${i+1}) ${o.k==='del'?'Borrar':'Guardar'} en ${o.t}${o.d&&!Array.isArray(o.d)&&o.d.descripcion?' — '+o.d.descripcion:''}\n   ${o.err}`).join('\n\n')||'Sin errores');}
function bannerSync(){
  const aviso=authNecesaria?`<div class="alrt" style="display:block">🔒 Este dispositivo tiene que <b>volver a iniciar sesión</b>. Lo que cargues queda guardado acá y se sube cuando ingreses.${OB.length?` (${OB.length} pendiente${OB.length===1?'':'s'})`:''}
    <button class="btn btnp" onclick="pedirReingreso()" style="width:100%;margin-top:8px;padding:6px">Iniciar sesión</button></div>`:'';
  if(!OBerr.length)return aviso;
  return aviso+`<div class="alrt" style="display:block">⚠ ${OBerr.length} registro(s) no se pudieron guardar en la nube (la base los rechazó). Siguen guardados en este celular.
    <div style="display:flex;gap:6px;margin-top:8px"><button class="btn" onclick="reintentarErrores()" style="flex:1;padding:6px">↻ Reintentar</button><button class="btn" onclick="verErrores()" style="flex:1;padding:6px">Ver detalle</button>${sesion?.rol==='dueno'?`<button class="btn" onclick="descartarErrores()" style="flex:1;padding:6px;color:var(--rd)">Descartar</button>`:''}</div></div>`;
}
// Vuelve a aplicar sobre los datos recién bajados lo que todavía no subió (así no desaparece de la pantalla)
function aplicarPendiente(op){
  const porId=(arr,row)=>{const i=arr.findIndex(x=>x.id===row.id);if(i>=0)arr[i]={...arr[i],...row};else arr.push({...row});};
  const porDia=(map,row)=>{const d=row.day;if(!map[d])map[d]=[];porId(map[d],row);};
  const lista={stock_groups:'sg',stock_variants:'vr',compras:'co',compras_items:'coi',cortes:'ct',cortes_items:'cti',elaboraciones:'el',elaboraciones_items:'eli',insumos:'ins',clientes_cc:'ccl',cliente_precios:'cp',usuarios:'us',retiros_stock:'rt'};
  const dias={ventas:'ve',gastos:'ga',caja_movimientos:'caja'};
  if(op.k==='up'){
    const rows=Array.isArray(op.d)?op.d:[op.d];
    rows.forEach(row=>{
      if(lista[op.t]){const r2=op.t==='insumos'?{...row,costUnit:row.cost_unit??0}:row;porId(S[lista[op.t]],r2);}
      else if(dias[op.t])porDia(S[dias[op.t]],row);
      else if(op.t==='cierres'){const d=row.day;S.cierres[d]={...(S.cierres[d]||{}),...row,detalle:parseDetalle(row.detalle)};}
    });
  }else{
    if(lista[op.t])S[lista[op.t]]=S[lista[op.t]].filter(x=>x.id!==op.id);
    else if(dias[op.t])Object.keys(S[dias[op.t]]).forEach(d=>{S[dias[op.t]][d]=S[dias[op.t]][d].filter(x=>x.id!==op.id);});
  }
}
function parseDetalle(d){for(let i=0;i<3&&typeof d==='string';i++){try{d=JSON.parse(d)}catch{return{}}}return d&&typeof d==='object'?d:{};}
// La sesión de login vive en sessionStorage: se borra sola al cerrar la pestaña/navegador
const SC={g(k){try{return JSON.parse(sessionStorage.getItem('tcc1_'+k))||null}catch{return null}},s(k,v){sessionStorage.setItem('tcc1_'+k,JSON.stringify(v))}};

let S={
  us:LC.g('us')||[],sg:LC.g('sg')||[],vr:LC.g('vr')||[],
  ve:LC.g('ve')||{},caja:LC.g('caja')||{},
  co:LC.g('co')||[],coi:LC.g('coi')||[],
  ct:LC.g('ct')||[],cti:LC.g('cti')||[],
  el:LC.g('el')||[],eli:LC.g('eli')||[],
  ga:LC.g('ga')||{},ins:LC.g('ins')||[],
  cierres:LC.g('cierres')||{},
  ccl:LC.g('ccl')||[],
  cp:LC.g('cp')||[],
  rt:LC.g('rt')||[],
  cfg:LC.g('cfg')||{},
};
// Merge defaults: completar los campos que falten en cfg
if(!S.cfg.mp||!Array.isArray(S.cfg.mp)||!S.cfg.mp.length)S.cfg.mp=[
  {id:'qr_mp',label:'QR Mercado Pago',pct:3.99},
  {id:'qr_otro',label:'QR otro medio',pct:2.50},
  {id:'debito',label:'Tarjeta débito',pct:1.10},
  {id:'credito_1c',label:'Tarjeta crédito (1 cuota)',pct:4.00},
  {id:'credito_3c',label:'Tarjeta crédito (3 cuotas)',pct:8.50},
  {id:'transferencia',label:'Transferencia bancaria',pct:0},
];
if(!S.cfg.gasCats||!Array.isArray(S.cfg.gasCats)||!S.cfg.gasCats.length)S.cfg.gasCats=[
  'Alquiler','Servicios','Personal','Embalaje','Limpieza','Comisiones digitales','Otros'
];
if(S.cfg.cierreDigital===undefined)S.cfg.cierreDigital=false; // apagado mientras el dueño use MP para gastos personales
LC.s('cfg',S.cfg);

let tab='caja',day=arDay(),online=navigator.onLine;
let sesion=SC.g('sesion')||null;
let ticketItems=[],corteItems=[],elabItems=[],compraItems=[];
let pagoSeleccionado='Efectivo';
let tkClienteId=null,tkHora='';
let ccClienteId=null,ccTicketAbierto=null,ccSeleccionados=new Set();
let charts={},rMonth=arMonth(),rTab='dia',prodTab='corte',comprasTab='mercaderia',cmpA=null,cmpB=null,cmpMetric='monto';
let loginRol='dueno',pinBuf='';
let ultimoDiaVisto=arDay(),ultimaCarga=0;

const BILLETES=[20000,10000,2000,1000,500,200,100,50];
const UNITS=['kg','unidad','litro','docena','bandeja','bolsa','gramo','paquete'];
// Categorías de los gastos del local (no pasan por la caja del día)
const CATS_LOCAL=['Alquiler','Luz','Gas','Agua','Internet / teléfono','Arreglos / mantenimiento','Impuestos / tasas','Seguros','Contador','Otros del local'];
// Costo de cada producto: promedio de los lotes (compras, cortes, elaboraciones) de los últimos N días, ponderado por kg
const COSTO_VENTANA_DIAS=30;
// Rinde: aviso si una elaboración se aleja más de este % del promedio de las anteriores del mismo producto
const RINDE_TOLERANCIA=0.15;

/* UTILS */
function arNow(){return new Date(new Date().toLocaleString('en-US',{timeZone:'America/Argentina/Buenos_Aires'}))}
function arDay(){const d=arNow();return d.getFullYear()+'-'+(d.getMonth()+1).toString().padStart(2,'0')+'-'+d.getDate().toString().padStart(2,'0')}
function arMonth(){const d=arNow();return d.getFullYear()+'-'+(d.getMonth()+1).toString().padStart(2,'0')}
function arTime(){return arNow().toLocaleTimeString('es-AR',{hour:'2-digit',minute:'2-digit'})}
function getPrevDay(d){const dt=new Date(d+'T12:00:00');dt.setDate(dt.getDate()-1);return dt.toISOString().split('T')[0]}
function fD(s){if(!s)return'';const[,m,d]=s.split('-');return d+'/'+m}
function fDL(s){if(!s)return'';const[y,m,d]=s.split('-');return d+'/'+m+'/'+y}
function fM(s){if(!s)return'';const[y,m]=s.split('-');return['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'][+m-1]+' '+y}
function $m(n){return'$'+Math.round(n||0).toLocaleString('es-AR')}
function $d2(n){return'$'+(+(n||0)).toFixed(2)}
function fQ(n,u){return(+(n||0)).toFixed(2).replace(/\.00$/,'')+(u?' '+u:'')}
function uid(){return Date.now().toString(36)+Math.random().toString(36).slice(2,5)}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
function sumarDias(d,n){const dt=new Date(d+'T12:00:00');dt.setDate(dt.getDate()+n);return dt.toISOString().split('T')[0]}
// "14:05" → "02:05 p. m." (mismo formato que arTime, para que funcione la separación por turnos)
function fmtHora(hhmm){const m=/^(\d{1,2}):(\d{2})$/.exec(hhmm||'');if(!m)return arTime();let h=+m[1];const pm=h>=12;h=h%12||12;return h.toString().padStart(2,'0')+':'+m[2]+' '+(pm?'p. m.':'a. m.');}

/* ══ COSTO POR LOTES ═══════════════════════════════════════════════
   El costo de un producto NO depende del stock (que puede ser negativo).
   Se calcula como el promedio de los lotes que entraron en los últimos
   30 días (compras de stock de venta, cortes costeados y elaboraciones),
   ponderado por kg: un lote de 30 kg pesa más que uno de 5 kg.
   Si cargás el costo a mano, el promedio arranca a contar desde ese día.
   Si no hay lotes en la ventana, se usa el último lote cargado.          */
function lotesGrupo(gid){
  const L=[];
  S.coi.forEach(i=>{
    if(i.tipo_destino!=='stock_venta'||i.ref_id!==gid||i.upd_cost===false||!(i.cost_unit_calculado>0))return;
    const c=S.co.find(x=>x.id===i.compra_id);if(c)L.push({day:c.day,qty:+(i.qty_real||i.qty_compra)||0,cu:+i.cost_unit_calculado,src:'Compra '+c.proveedor});
  });
  S.cti.forEach(i=>{
    if(i.group_id!==gid)return;
    const c=S.ct.find(x=>x.id===i.corte_id);
    // corte con lote de cajón: el ítem es un lote aunque su costo sea 0 (subproducto)
    if(c&&(i.cost_unit_aplicado>0||c.origen_compra_item_id))L.push({day:c.day,qty:+i.qty||0,cu:+i.cost_unit_aplicado||0,src:'Corte '+c.nombre});
  });
  S.el.forEach(e=>{
    if(e.output_group_id!==gid||!(e.output_qty>0)||!(e.costo_total_info>0))return;
    L.push({day:e.day,qty:+e.output_qty,cu:e.costo_total_info/e.output_qty,src:'Elab. '+e.nombre});
  });
  return L.filter(l=>l.qty>0).sort((a,b)=>b.day.localeCompare(a.day));
}
function lotesVigentes(g){
  let L=lotesGrupo(g.id);
  const man=g.cost_manual_at?String(g.cost_manual_at).slice(0,10):null;
  if(man)L=L.filter(l=>l.day>=man);
  if(!L.length)return[];
  const desde=sumarDias(arDay(),-COSTO_VENTANA_DIAS);
  const W=L.filter(l=>l.day>=desde);
  return W.length?W:[L[0]];
}
function recalcCosto(g){
  if(g.subproducto){g.cost_unit=0;return;} // subproducto: su costo ya lo pagan los cortes principales
  const W=lotesVigentes(g);
  if(!W.length)return; // sin lotes: queda el costo que tenía (el manual o el anterior)
  const kg=W.reduce((s,l)=>s+l.qty,0);
  g.cost_unit=W.reduce((s,l)=>s+l.qty*l.cu,0)/kg;
}
// Filas completas para subir (siempre los mismos campos)
function sgRow(g){const r={id:g.id,name:g.name,unit:g.unit,tipo:g.tipo,stock_qty:g.stock_qty||0,cost_unit:g.cost_unit||0,cost_manual_at:g.cost_manual_at||null};if(g.subproducto!==undefined)r.subproducto=!!g.subproducto;return r;}
function insRow(i){return{id:i.id,name:i.name,unit:i.unit,cost_unit:i.costUnit??i.cost_unit??0,stock_qty:i.stock_qty||0};}
function subirGrupos(ids){[...new Set(ids)].forEach(id=>{const g=S.sg.find(x=>x.id===id);if(g)sbUp('stock_groups',sgRow(g));});}
function subirInsumos(ids){[...new Set(ids)].forEach(id=>{const i=S.ins.find(x=>x.id===id);if(i)sbUp('insumos',insRow(i));});}
// Rinde de una elaboración: kg que salen por cada kg de materia prima (ingredientes de stock en kg)
function rindeElab(outQty,items){const kgMP=items.filter(x=>x.tipo==='stock'&&(x.unit||'kg')==='kg').reduce((s,x)=>s+(+x.qty||0),0);return kgMP>0&&outQty>0?outQty/kgMP:null;}
function rindePromedio(outGid,excluirId){
  const prev=S.el.filter(e=>e.output_group_id===outGid&&e.id!==excluirId).sort((a,b)=>(b.day+(b.created_at||'')).localeCompare(a.day+(a.created_at||''))).slice(0,5)
    .map(e=>rindeElab(e.output_qty,S.eli.filter(i=>i.elaboracion_id===e.id))).filter(r=>r);
  return prev.length?prev.reduce((s,r)=>s+r,0)/prev.length:null;
}
// Determina cuanto de un gasto corresponde a efectivo/transferencia (soporta gastos manuales viejos y compras con split)
function gastoEf(g){if(g.pago_efectivo||g.pago_transferencia)return g.pago_efectivo||0;return g.metodo==='transferencia'?0:g.amount;}
function gastoTr(g){if(g.pago_efectivo||g.pago_transferencia)return g.pago_transferencia||0;return g.metodo==='transferencia'?g.amount:0;}
function dV(){return S.ve[day]||[]}
function dG(){return S.ga[day]||[]}
function dCaja(){return S.caja[day]||[]}
function sgV(){return S.sg.filter(g=>g.tipo==='venta'||!g.tipo)}
function sgP(){return S.sg.filter(g=>g.tipo==='produccion')}
function toast(m){const t=document.getElementById('toast');t.textContent=m;t.classList.add('on');setTimeout(()=>t.classList.remove('on'),2400)}
function sync(s,l){const d=document.getElementById('sdot'),lb=document.getElementById('slbl');if(d){d.className='sdot '+s;lb.textContent=l}}
function save(){LC.s('us',S.us);LC.s('sg',S.sg);LC.s('vr',S.vr);LC.s('ve',S.ve);LC.s('caja',S.caja);LC.s('co',S.co);LC.s('coi',S.coi);LC.s('ct',S.ct);LC.s('cti',S.cti);LC.s('el',S.el);LC.s('eli',S.eli);LC.s('ga',S.ga);LC.s('ins',S.ins);LC.s('cierres',S.cierres);LC.s('ccl',S.ccl);LC.s('cp',S.cp);LC.s('rt',S.rt);LC.s('cfg',S.cfg);}
function toggleDetail(id){const el=document.getElementById(id);if(el)el.style.display=el.style.display==='none'?'block':'none';}
// Gastos que salen del cajón del día (no los de compras ni los del local)
function esGastoCaja(g,compraGastoIds){return!compraGastoIds.has(g.id)&&!g.fuera_caja;}
function idsGastoCompra(){return new Set(S.co.map(c=>c.gasto_id).filter(Boolean));}

window.addEventListener('online',()=>{online=true;flush();loadAll()});
window.addEventListener('offline',()=>{online=false;updSync()});
// Al volver a la app: subir lo pendiente, pasar a "hoy" si cambió el día, y refrescar si pasó un rato
function alVolver(){
  if(!sesion)return;
  flush();
  const hoy=arDay();
  if(hoy!==ultimoDiaVisto){if(day===ultimoDiaVisto){day=hoy;tkHora='';}ultimoDiaVisto=hoy;render();}
  if(Date.now()-ultimaCarga>5*60*1000)loadAll();
}
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')alVolver();});
window.addEventListener('focus',alVolver);
setInterval(()=>{if(OB.length)flush();},20000);
setInterval(()=>{if(sesion&&arDay()!==ultimoDiaVisto)alVolver();},60000);

/* LOGIN */
function selUser(r){loginRol=r;pinBuf='';document.getElementById('usel-dueno').classList.toggle('active',r==='dueno');document.getElementById('usel-empleado').classList.toggle('active',r==='empleado');upPD();}
function pinPress(d){if(pinBuf.length>=6)return;pinBuf+=d;upPD();}
function pinClear(){pinBuf='';upPD();document.getElementById('pin-error').textContent='';}
function upPD(){const el=document.getElementById('pin-display');if(el)el.textContent=pinBuf?'●'.repeat(pinBuf.length):'····';}
async function pinOk(){
  if(!pinBuf){document.getElementById('pin-error').textContent='Ingresá tu PIN';return;}
  let usr=S.us.find(u=>u.rol===loginRol&&u.pin===pinBuf&&u.activo!==false);
  if(!usr){document.getElementById('pin-error').textContent='PIN incorrecto';pinBuf='';upPD();return;}
  sesion={id:usr.id,nombre:usr.nombre,rol:usr.rol};SC.s('sesion',sesion);
  document.getElementById('login-screen').style.display='none';
  document.getElementById('app-screen').style.display='block';
  initApp();
}
function doLogout(){sesion=null;SC.s('sesion',null);pinBuf='';upPD();document.getElementById('login-screen').style.display='flex';document.getElementById('app-screen').style.display='none';}

const NAV_D=[{id:'caja',i:'💰',l:'Caja'},{id:'stock',i:'📦',l:'Stock'},{id:'prod',i:'🔪',l:'Prod.'},{id:'compras',i:'🛒',l:'Compras'},{id:'cc',i:'👥',l:'Clientes'},{id:'reportes',i:'📊',l:'Reportes'}];
const NAV_E=[{id:'caja',i:'💰',l:'Caja'},{id:'gastos',i:'🧾',l:'Gastos'}];

function buildNav(){
  const items=sesion?.rol==='dueno'?NAV_D:NAV_E;
  document.getElementById('bottom-nav').innerHTML=items.map(n=>`<div class="bni ${n.id===tab?'active':''}" id="bn-${n.id}" onclick="go('${n.id}')"><div class="bi">${n.i}</div><div class="bl">${n.l}</div></div>`).join('');
}
function go(t){tab=t;document.querySelectorAll('.bni').forEach(el=>el.classList.toggle('active',el.id==='bn-'+t));render();}
function chDay(d){const dt=new Date(day+'T12:00:00');dt.setDate(dt.getDate()+d);const nd=dt.toISOString().split('T')[0];if(nd>arDay())return;day=nd;render();}
function goToDate(v){if(!v)return;if(v>arDay())v=arDay();day=v;render();}
function abrirCalendario(){
  const dp=document.getElementById('day-picker');
  if(!dp)return;
  if(dp.showPicker){try{dp.showPicker();}catch(e){dp.focus();dp.click();}}
  else{dp.focus();dp.click();}
}

let appIniciada=false;
function initApp(){appIniciada=true;day=arDay();ultimoDiaVisto=day;buildNav();updSync();render();flush();loadAll();}

async function loadAll(){
  sync('busy','cargando...');
  try{
    await flush(); // primero subir lo pendiente
    const[us,sg,vr,ve,caja,co,coi,ct,cti,el,eli,ga,ins,cierresArr,ccl,cp,rt]=await Promise.all([
      sbQ('usuarios','order=id'),sbQ('stock_groups','order=name'),sbQ('stock_variants','order=name'),
      sbQ('ventas','order=created_at,id'),sbQ('caja_movimientos','order=created_at,id'),
      sbQ('compras','order=created_at,id'),sbQ('compras_items','order=created_at,id'),
      sbQ('cortes','order=created_at,id'),sbQ('cortes_items','order=created_at,id'),
      sbQ('elaboraciones','order=created_at,id'),sbQ('elaboraciones_items','order=created_at,id'),
      sbQ('gastos','order=created_at,id'),sbQ('insumos','order=name'),
      sbQ('cierres','order=day'),
      sbQ('clientes_cc','order=nombre'),
      sbQ('cliente_precios','order=id').catch(()=>[]), // tabla nueva: si todavía no se corrió la migración, sigue funcionando
      sbQ('retiros_stock','order=created_at,id').catch(()=>null), // idem: tabla nueva de retiros
    ]);
    S.us=us;S.sg=sg;S.vr=vr;S.co=co;S.coi=coi;S.ct=ct;S.cti=cti;S.el=el;S.eli=eli;
    S.ccl=ccl||[];S.cp=cp||[];if(rt)S.rt=rt;
    S.ins=ins.map(i=>({...i,costUnit:i.cost_unit||0,stock_qty:i.stock_qty||0}));
    const cm2={};
    (cierresArr||[]).forEach(c=>{
      const d=typeof c.day==='string'?c.day.slice(0,10):c.day;
      cm2[d]={total_contado:c.total_contado,retiro:c.retiro,saldo_siguiente:c.saldo_siguiente,fondo_inicial_manual:c.fondo_inicial_manual,fondo_digital_manual:c.fondo_digital_manual,saldo_digital_real:c.saldo_digital_real,saldo_digital_siguiente:c.saldo_digital_siguiente,detalle:parseDetalle(c.detalle),time:c.time};
    });
    S.cierres=cm2;
    const vm={},gm={},cm={};
    ve.forEach(x=>{if(!vm[x.day])vm[x.day]=[];vm[x.day].push(x)});
    // Restaurar metodo en gastos (Supabase puede no tenerlo si la columna es nueva)
    ga.forEach(x=>{
      if(!x.metodo)x.metodo=x.cat==='Comisiones digitales'?'transferencia':'efectivo';
      if(!gm[x.day])gm[x.day]=[];gm[x.day].push(x);
    });
    caja.forEach(x=>{if(!cm[x.day])cm[x.day]=[];cm[x.day].push(x)});
    S.ve=vm;S.ga=gm;S.caja=cm;
    // Lo que todavía no subió (sin conexión o rechazado) se vuelve a aplicar encima: no desaparece de la pantalla
    [...OBerr,...OB].forEach(aplicarPendiente);
    ultimaCarga=Date.now();
    save();updSync();render();
  }catch(e){updSync();if(authNecesaria)render();else if(!OB.length&&!OBerr.length)sync('err','sin conexión');console.error(e)}
}

function render(){
  if(!sesion)return;
  const isT=day===arDay();
  document.getElementById('hdr-sub').textContent=(isT?'Hoy · ':'')+fDL(day)+' · '+sesion.nombre;
  document.getElementById('dnd').textContent=isT?'Hoy':fD(day);
  const dp=document.getElementById('day-picker');if(dp){dp.value=day;dp.max=arDay();}
  Object.values(charts).forEach(c=>{try{c.destroy()}catch(e){}});charts={};
  const c=document.getElementById('content');
  if(tab==='caja')c.innerHTML=bannerSync()+rCaja();
  else if(tab==='stock')c.innerHTML=bannerSync()+rStock();
  else if(tab==='prod'){c.innerHTML=bannerSync()+rProd();if(prodTab==='corte')renderCorteItems();else if(prodTab==='elab')renderElabItems();}
  else if(tab==='compras'){c.innerHTML=bannerSync()+rCompras();if(comprasTab==='mercaderia')renderCompraItems();}
  else if(tab==='cc')c.innerHTML=bannerSync()+rCC();
  else if(tab==='gastos')c.innerHTML=bannerSync()+rGastos();
  else if(tab==='reportes'){c.innerHTML=bannerSync()+rReportes();initCharts();}
}
// Convierte "11:28 a. m." / "1:15 p. m." a minutos desde medianoche, para separar turnos y ordenar
function timeToMin(t){
  if(!t)return -1;
  const m=String(t).match(/(\d{1,2}):(\d{2})\s*([ap])/i);
  if(!m)return -1;
  let h=parseInt(m[1]),min=parseInt(m[2]);
  const pm=m[3].toLowerCase()==='p';
  if(pm&&h!==12)h+=12;
  if(!pm&&h===12)h=0;
  return h*60+min;
}
// Precio de una variante para el cliente elegido (precio especial si tiene, si no el de lista)
function precioCliente(clienteId,varId){
  const vr=S.vr.find(x=>x.id===varId);
  const esp=clienteId?S.cp.find(p=>p.cliente_id===clienteId&&p.variant_id===varId):null;
  return esp?+esp.price:+(vr?.price||0);
}
function nombreCliente(id){return S.ccl.find(c=>c.id===id)?.nombre||'';}

/* ══ CAJA ══════════════════════════════════════════════════════ */
function rCaja(){
  const vs=dV(),tv=vs.reduce((s,v)=>s+v.total,0);
  const tvCaja=vs.filter(v=>v.pago!=='cuenta_corriente').reduce((s,v)=>s+v.total,0); // ventas que sí mueven caja hoy (excluye Cta.Cte hasta que se cobre)
  const{ef}=calcEfTr(vs);
  const movs=dCaja();
  const compraGastoIds=idsGastoCompra();
  const gasOp=dG().filter(g=>esGastoCaja(g,compraGastoIds));
  const ingEf=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
  const egEf=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
  const ingTr=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='transferencia').reduce((s,m)=>s+m.monto,0);
  const egTr=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='transferencia').reduce((s,m)=>s+m.monto,0);
  const ga=gasOp.reduce((s,g)=>s+g.amount,0);
  const cajaEf=ef+ingEf-egEf;
  const prevDay=getPrevDay(day);
  const cierrePrev=S.cierres?.[prevDay];
  const cierreHoy=S.cierres?.[day];
  const fondoInicial=cierreHoy?.fondo_inicial_manual??cierrePrev?.saldo_siguiente??0;
  const fondoDigital=cierreHoy?.fondo_digital_manual??cierrePrev?.saldo_digital_siguiente??0;
  const cajaTr=(tvCaja-ef)+ingTr-egTr+(S.cfg.cierreDigital?fondoDigital:0);
  const resultado=tvCaja+ingEf+ingTr-egEf-egTr-ga;
  // Recordatorio de cierre digital: hoy (si ya cerró el efectivo) o el día anterior con ventas
  const faltaDigHoy=S.cfg.cierreDigital&&cierreHoy&&cierreHoy.saldo_digital_real==null;
  const faltaDigAyer=S.cfg.cierreDigital&&(S.ve[prevDay]||[]).length>0&&S.cierres?.[prevDay]?.saldo_digital_real==null;
  const esHoy=day===arDay();

  // ticket en proceso
  const tktTotal=ticketItems.reduce((s,x)=>s+x.total,0);
  const vrOpts=S.vr.map(v=>{const g=sgV().find(x=>x.id===v.group_id);const p=precioCliente(tkClienteId,v.id);return`<option value="${v.id}" data-p="${p}" data-k="${v.qty_per_unit}">${esc(g?g.name+' › ':'')}${esc(v.name)}${p!==+(v.price||0)?' ★':''}</option>`}).join('');
  const cliOpts=[...S.ccl].sort((a,b)=>a.nombre.localeCompare(b.nombre)).map(c=>`<option value="${c.id}" ${c.id===tkClienteId?'selected':''}>${esc(c.nombre)}</option>`).join('');

  // ventas del dia agrupadas por ticket_id
  const byTicket={};
  vs.forEach(v=>{const tk=v.ticket_id||v.id;if(!byTicket[tk])byTicket[tk]={items:[],total:0,pago:'',time:v.time||'',usuario:v.usuario||'',cliente:v.cliente_id||v.cliente_cc_id||''};byTicket[tk].items.push(v);byTicket[tk].total+=v.total;byTicket[tk].pago=v.pago||'';if(!byTicket[tk].created_at)byTicket[tk].created_at=v.created_at||v.time||'';});
  // Ordenar por hora de la venta (así las cargas atrasadas con hora real quedan en su lugar)
  const ticketsSorted=Object.entries(byTicket).sort((a,b)=>(timeToMin(a[1].time)-timeToMin(b[1].time))||String(a[1].created_at).localeCompare(String(b[1].created_at)));
  const CORTE_TURNO=13*60+30; // 13:30
  const manana=ticketsSorted.filter(([,tk])=>timeToMin(tk.time)<CORTE_TURNO);
  const tarde=ticketsSorted.filter(([,tk])=>timeToMin(tk.time)>=CORTE_TURNO);
  function subtotalRow(label,list){
    const tot=list.reduce((s,[,tk])=>s+tk.total,0);
    return`<tr style="background:var(--sf2)"><td colspan="3" style="font-weight:600;font-size:11px;color:var(--tx2)">${label} (${list.length})</td><td colspan="3" style="font-weight:700;font-family:var(--mo)">${$m(tot)}</td></tr>`;
  }
  function ticketRow([tid,tk],idx){
    const itemList=tk.items.map(v=>{const vr=S.vr.find(x=>x.id===v.variant_id),gr=S.sg.find(x=>x.id===v.group_id);return`<div style="font-size:10px;color:var(--tx3);padding:1px 0">${esc(gr?gr.name:'–')}${vr?' › '+esc(vr.name):''} ×${v.qty} = ${$m(v.total)}</div>`;}).join('');
    const pagoTag=tk.pago==='mixto'?`<span class="tag tmx">mix</span>`:tk.pago==='Transferencia'?`<span class="tag tp">Tr.</span>`:tk.pago==='cuenta_corriente'?`<span class="tag" style="background:rgba(167,139,250,.15);color:var(--pu,#a78bfa)">CtaCte</span>`:`<span class="tag tv">Ef.</span>`;
    return`<tr>
      <td style="font-family:var(--mo);color:var(--tx3);font-size:11px">#${idx+1}</td>
      <td>${tk.time}${tk.usuario?`<div style="font-size:9px;color:var(--tx3)">${esc(tk.usuario)}</div>`:''}</td>
      <td><div>${tk.cliente?`<div style="font-size:10px;color:var(--ac);font-weight:600">👤 ${esc(nombreCliente(tk.cliente)||'Cliente')}</div>`:''}${itemList}</div></td>
      <td style="font-weight:500">${$m(tk.total)}</td>
      <td>${pagoTag}</td>
      <td>${sesion?.rol==='dueno'?`<button class="dbtn" onclick="delTicket('${tid}')">✕</button>`:''}</td>
    </tr>`;
  }
  const vRows=ticketsSorted.length?[
    ...manana.map((t,i)=>ticketRow(t,i)),
    manana.length?subtotalRow('Subtotal turno mañana',manana):'',
    ...tarde.map((t,i)=>ticketRow(t,manana.length+i)),
    tarde.length?subtotalRow('Subtotal turno tarde',tarde):''
  ].join(''):`<tr><td colspan="6" class="empty-row">Sin ventas</td></tr>`;

  const gaEf=gasOp.reduce((s,g)=>s+gastoEf(g),0);
  const gaTr=gasOp.reduce((s,g)=>s+gastoTr(g),0);
  const gasRows=gasOp.length?gasOp.map(g=>`<tr><td>${g.time||''}${g.usuario?`<div style="font-size:9px;color:var(--tx3)">${esc(g.usuario)}</div>`:''}</td><td style="max-width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(g.descripcion)}</td><td><span class="tag tg">${(g.cat||'').slice(0,5)}</span></td><td>${$m(g.amount)}</td><td><span class="tag ${g.metodo==='transferencia'?'tp':'tv'}">${g.metodo==='transferencia'?'Tr.':'Ef.'}</span></td><td><button class="dbtn" onclick="delGa('${g.id}')">✕</button></td></tr>`).join(''):`<tr><td colspan="6" class="empty-row">Sin gastos</td></tr>`;

  const mRows=movs.length?movs.map(m=>`<tr><td>${m.time||''}</td><td>${esc(m.descripcion)}</td><td><span class="tag ${m.tipo==='ingreso'?'tv':'tg'}">${m.tipo}</span></td><td style="color:${m.tipo==='ingreso'?'var(--gn)':'var(--rd)'}">${m.tipo==='ingreso'?'+':'-'}${$m(m.monto)}</td><td><span class="tag tp">${m.metodo.slice(0,3)}</span></td><td><button class="dbtn" onclick="delMov('${m.id}')">✕</button></td></tr>`).join(''):`<tr><td colspan="6" class="empty-row">Sin movimientos</td></tr>`;

  return`
  <div class="kpis">
    <div class="kc hi"><div class="kl">Ventas</div><div class="kv a">${$m(tvCaja)}</div><div class="kh">${vs.filter(v=>v.pago!=='cuenta_corriente').length} ítems · ${new Set(vs.filter(v=>v.pago!=='cuenta_corriente').map(v=>v.ticket_id||v.id)).size} tickets${vs.length!==vs.filter(v=>v.pago!=='cuenta_corriente').length?' · +Cta.Cte':''}</div></div>
    <div class="kc"><div class="kl">Resultado</div><div class="kv ${resultado>=0?'g':'r'}">${$m(resultado)}</div></div>
    <div class="kc" style="border-color:rgba(26,158,58,.3);background:rgba(26,158,58,.04)"><div class="kl">Efectivo caja</div><div class="kv" style="color:var(--gn)">${$m(cajaEf)}</div></div>
    <div class="kc" style="border-color:rgba(46,155,212,.3);background:rgba(46,155,212,.04)"><div class="kl">Transferencias</div><div class="kv" style="color:var(--bl)">${$m(cajaTr)}</div></div>
  </div>
  ${fondoInicial>0?`<div class="info-box green">💵 Fondo inicial del día: ${$m(fondoInicial)}</div>`:''}
  ${faltaDigAyer?`<div class="info-box amber">📱 El ${fD(prevDay)} quedó sin cierre digital (Mercado Pago). Andá a ese día y cargá el saldo que muestra MP.</div>`:''}
  ${faltaDigHoy?`<div class="info-box amber">📱 Ya cerraste el efectivo, pero <b>falta el cierre digital</b> de este día. Está al final de la pantalla.</div>`:''}

  <!-- TICKET -->
  <div class="blk" style="border-color:rgba(208,2,27,.35)">
    <div class="bt">🧾 Ticket en proceso</div>
    <div class="fr">
      <div class="fl" style="flex:3"><label>Cliente (opcional)</label>
        <select id="tk-cliente" onchange="onTKCliente(this.value)">
          <option value="">— Consumidor final —</option>
          ${cliOpts}
          <option value="__nuevo">+ Nuevo cliente…</option>
        </select>
      </div>
      ${!esHoy?`<div class="fl" style="max-width:100px"><label>Hora real</label><input type="time" id="tk-hora" value="${tkHora}" onchange="tkHora=this.value"></div>`:''}
    </div>
    ${!esHoy?`<div style="font-size:10px;color:var(--ac);font-family:var(--mo);margin:-2px 0 8px">⏱ Estás cargando ventas del ${fDL(day)}. Poné la hora real de cada venta para que los turnos queden bien.</div>`:''}
    <div class="fr">
      <div class="fl" style="flex:3"><label>Producto</label>
        <select id="tk-var" onchange="onTKVar()">${vrOpts||'<option value="">Sin variantes</option>'}</select>
      </div>
      <div class="fl" style="max-width:58px"><label>Cant.</label>
        <input type="number" id="tk-qty" value="1" min="0.1" step="0.1" oninput="calcTK()">
      </div>
    </div>
    <div class="fr">
      <div class="fl"><label>Precio $</label><input type="number" id="tk-price" placeholder="0" oninput="calcTK()"></div>
      <div class="fl" style="max-width:70px"><label>Desc %</label><input type="number" id="tk-desc" placeholder="0" min="0" max="100" oninput="calcTK()"></div>
      <div class="fl"><label>Subtotal</label><input type="text" id="tk-sub" readonly style="color:var(--ac)"></div>
      <button class="btn" onclick="addTKItem()" style="align-self:flex-end;background:var(--sf2)">+ Ítem</button>
    </div>
    <!-- items del ticket en proceso -->
    <div id="tk-items"></div>
    ${ticketItems.length?`
    <div class="sep"></div>
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
      <span style="font-size:13px;font-weight:600">Total ticket</span>
      <span style="font-size:20px;font-weight:700;font-family:var(--mo);color:var(--ac)">${$m(tktTotal)}</span>
    </div>
    <div style="font-size:9px;color:var(--tx2);font-family:var(--mo);margin-bottom:7px">FORMA DE PAGO</div>
    <div style="display:flex;gap:7px;margin-bottom:10px">
      <button id="pago-btn-ef" onclick="selPago('Efectivo')" style="flex:1;padding:9px 4px;border-radius:8px;border:1.5px solid ${pagoSeleccionado==='Efectivo'?'var(--gn)':'var(--br2)'};background:${pagoSeleccionado==='Efectivo'?'rgba(74,222,128,.12)':'var(--sf2)'};color:${pagoSeleccionado==='Efectivo'?'var(--gn)':'var(--tx2)'};font-size:12px;font-weight:700;cursor:pointer;font-family:var(--sa)">💵 Efectivo</button>
      <button id="pago-btn-tr" onclick="selPago('Transferencia')" style="flex:1;padding:9px 4px;border-radius:8px;border:1.5px solid ${pagoSeleccionado==='Transferencia'?'var(--bl)':'var(--br2)'};background:${pagoSeleccionado==='Transferencia'?'rgba(96,165,250,.12)':'var(--sf2)'};color:${pagoSeleccionado==='Transferencia'?'var(--bl)':'var(--tx2)'};font-size:12px;font-weight:700;cursor:pointer;font-family:var(--sa)">📲 Transf.</button>
      <button id="pago-btn-mix" onclick="selPago('mixto')" style="flex:1;padding:9px 4px;border-radius:8px;border:1.5px solid ${pagoSeleccionado==='mixto'?'var(--ac)':'var(--br2)'};background:${pagoSeleccionado==='mixto'?'rgba(232,118,10,.12)':'var(--sf2)'};color:${pagoSeleccionado==='mixto'?'var(--ac)':'var(--tx2)'};font-size:12px;font-weight:700;cursor:pointer;font-family:var(--sa)">🔀 Mixto</button>
      <button id="pago-btn-cc" onclick="selPago('cuenta_corriente')" style="flex:1;padding:9px 4px;border-radius:8px;border:1.5px solid ${pagoSeleccionado==='cuenta_corriente'?'var(--pu,#a78bfa)':'var(--br2)'};background:${pagoSeleccionado==='cuenta_corriente'?'rgba(167,139,250,.12)':'var(--sf2)'};color:${pagoSeleccionado==='cuenta_corriente'?'var(--pu,#a78bfa)':'var(--tx2)'};font-size:12px;font-weight:700;cursor:pointer;font-family:var(--sa)">📒 Cta.Cte</button>
    </div>
    <div id="tk-cc-field" style="display:${pagoSeleccionado==='cuenta_corriente'?'block':'none'};margin-bottom:10px;font-size:11px;font-family:var(--mo);color:${tkClienteId?'var(--tx2)':'var(--rd)'}">
      ${tkClienteId?`📒 Va a la cuenta corriente de <b>${esc(nombreCliente(tkClienteId))}</b>`:'📒 Elegí el cliente arriba (en "Cliente") para cargarlo a su cuenta corriente'}
    </div>
    <div id="tk-mixto-fields" style="display:${pagoSeleccionado==='mixto'?'block':'none'}">
      <div class="fr">
        <div class="fl"><label>Transferencia $</label><input type="number" id="tk-pago-tr" placeholder="0" oninput="calcTKMixto()"></div>
        <div class="fl"><label>Efectivo (el resto)</label><input type="text" id="tk-pago-ef-calc" readonly style="color:var(--gn)"></div>
      </div>
    </div>
    <div id="tk-mp-field" style="display:${pagoSeleccionado!=='Efectivo'&&pagoSeleccionado!=='cuenta_corriente'?'block':'none'};margin-bottom:8px">
      <label style="font-size:9px;color:var(--tx2);font-family:var(--mo);letter-spacing:.5px">MEDIO DE COBRO DIGITAL</label>
      <select id="tk-mp-tipo" style="margin-top:4px;width:100%">
        ${(S.cfg.mp||[]).map(m=>`<option value="${m.id}" data-pct="${m.pct}">${m.label} ${m.pct>0?'('+m.pct+'%)':''}</option>`).join('')}
      </select>
    </div>
    <button class="btn btnp" onclick="cerrarTicket()" style="width:100%">✓ Cerrar ticket</button>
    <button class="btn" onclick="cancelarTicket()" style="width:100%;margin-top:6px;background:var(--sf2);color:var(--rd);border-color:rgba(248,113,113,.4)">✕ Cancelar venta</button>
    `:'<div style="font-size:11px;color:var(--tx3);font-family:var(--mo);padding:6px 0">Agregá ítems para armar el ticket</div>'}
  </div>

  <div class="tbk"><div class="tt">Ventas del día (por ticket)</div>
    <table><thead><tr><th>#</th><th>Hora</th><th>Ítems</th><th>Total</th><th>Pago</th><th></th></tr></thead>
    <tbody>${vRows}</tbody></table>
  </div>

  <div class="blk" style="border-color:rgba(248,113,113,.3)">
    <div class="bt">Gastos de caja (plata que sale del cajón hoy)</div>
    <div style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin:-4px 0 8px">Alquiler, luz, arreglos y todo lo que no sale del cajón → Compras › Gastos del local</div>
    <div class="fr">
      <div class="fl" style="flex:2"><label>Descripción</label><input type="text" id="g-d" placeholder="Ej: bolsas, sueldo del día, viático..."></div>
      <div class="fl"><label>Categoría</label>
        <select id="g-c">${(S.cfg.gasCats||[]).map(c=>`<option>${esc(c)}</option>`).join('')}</select>
      </div>
    </div>
    <div class="fr">
      <div class="fl"><label>Monto $</label><input type="number" id="g-a" placeholder="0"></div>
      <div class="fl" style="max-width:110px"><label>Método</label><select id="g-met"><option value="efectivo">Efectivo</option><option value="transferencia">Transferencia</option></select></div>
      <button class="btn btnr" onclick="addGa()" style="align-self:flex-end">+ Gasto</button>
    </div>
  </div>
  <div class="tbk"><div class="tt">Gastos de caja del día</div>
    <table><thead><tr><th>Hora</th><th>Descripción</th><th>Cat.</th><th>Monto</th><th>Método</th><th></th></tr></thead>
    <tbody>${gasRows}</tbody></table>
  </div>

  <div class="blk"><div class="bt">Movimiento de caja (fondos / retiros extra)</div>
    <div class="fr">
      <div class="fl"><label>Tipo</label><select id="mov-tipo"><option value="ingreso">Ingreso</option><option value="egreso">Egreso</option></select></div>
      <div class="fl" style="flex:2"><label>Descripción</label><input type="text" id="mov-desc" placeholder="Ej: Fondo inicial, retiro..."></div>
    </div>
    <div class="fr">
      <div class="fl"><label>Monto $</label><input type="number" id="mov-monto" placeholder="0"></div>
      <div class="fl"><label>Método</label><select id="mov-metodo"><option value="efectivo">Efectivo</option><option value="transferencia">Transferencia</option></select></div>
      <button class="btn btnp" onclick="addMov()" style="align-self:flex-end">+ Agregar</button>
    </div>
  </div>
  <div class="tbk"><div class="tt">Movimientos de caja</div>
    <table><thead><tr><th>Hora</th><th>Descripción</th><th>Tipo</th><th>Monto</th><th>Método</th><th></th></tr></thead>
    <tbody>${mRows}</tbody></table>
  </div>

  <!-- CIERRE DE CAJA -->
  <div class="blk" style="border-color:rgba(232,118,10,.5)">
    <div class="bt">Cierre de caja — efectivo</div>
    ${sesion?.rol==='dueno'?`
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;padding:7px 10px;background:var(--sf2);border-radius:7px;border:1px solid var(--br)">
      <label style="display:flex;align-items:center;gap:5px;font-size:11px;cursor:pointer;font-family:var(--mo);color:var(--tx2);white-space:nowrap">
        <input type="checkbox" id="cierre-editar-fondo" style="width:auto;accent-color:var(--ac)" onchange="toggleFondoEdit()"> Fondo inicial del día:
      </label>
      <span id="cierre-fondo-display" style="font-family:var(--mo);font-size:12px;color:var(--gn)">${$m(fondoInicial)}</span>
      <input type="number" id="cierre-fondo-manual" placeholder="0" style="display:none;max-width:100px;font-size:12px" value="${S.cierres?.[day]?.fondo_inicial_manual??fondoInicial}" oninput="calcCierre()">
    </div>`:`<div style="font-size:10px;color:var(--tx2);font-family:var(--mo);margin-bottom:10px">Fondo inicial: <span style="color:var(--gn)">${$m(fondoInicial)}</span></div>`}
    <div style="font-size:10px;color:var(--tx2);font-family:var(--mo);margin-bottom:10px">Contá los billetes para calcular el total</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:10px">
      ${BILLETES.map(b=>`<div style="display:flex;align-items:center;gap:5px;background:var(--sf2);border-radius:5px;padding:5px 8px">
        <span style="font-size:10px;font-family:var(--mo);color:var(--tx3);min-width:62px">${$m(b)}</span>
        <input type="number" id="bill_${b}" min="0" value="0" step="1" style="width:50px;padding:3px 5px;font-size:13px" oninput="calcCierre()">
        <span style="font-size:10px;font-family:var(--mo);color:var(--ac);min-width:52px" id="bsub_${b}"></span>
      </div>`).join('')}
    </div>
    <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-top:1px solid var(--br);border-bottom:1px solid var(--br);margin-bottom:10px">
      <span style="font-size:12px;color:var(--tx2)">Total contado</span>
      <span style="font-size:20px;font-weight:700;font-family:var(--mo);color:var(--ac)" id="cierre-total">$0</span>
    </div>
    <div class="fr">
      <div class="fl"><label>Retiro del día $</label><input type="number" id="cierre-retiro" placeholder="0" value="${cierreHoy?.retiro||0}" oninput="calcCierre()"></div>
      <div class="fl"><label>Queda en caja</label><input type="text" id="cierre-saldo" readonly style="color:var(--gn);font-size:15px;font-weight:600"></div>
    </div>
    <div style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin:0 0 10px">El saldo pasa como fondo del día siguiente</div>
    <div style="background:var(--sf2);border-radius:8px;padding:10px;margin-bottom:10px;border:1px solid var(--br)">
      <div style="font-size:9px;color:var(--tx2);font-family:var(--mo);margin-bottom:7px;letter-spacing:.5px">CUENTA DEL DÍA — EFECTIVO</div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Fondo inicial</span><span style="font-family:var(--mo);color:var(--gn)" id="cierre-fondo-cuenta">+${$m(fondoInicial)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Ventas efectivo</span><span style="font-family:var(--mo);color:var(--gn)">+${$m(ef)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Ingresos/egresos manuales (ef.)</span><span style="font-family:var(--mo);color:${(ingEf-egEf)>=0?'var(--gn)':'var(--rd)'}" id="cierre-mov-cuenta">${(ingEf-egEf)>=0?'+':''}${$m(ingEf-egEf)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Gastos en efectivo</span><span style="font-family:var(--mo);color:var(--rd)">-${$m(gaEf)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Retiro</span><span style="font-family:var(--mo);color:var(--rd)" id="cierre-retiro-display">-${$m(cierreHoy?.retiro||0)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:5px 0 3px;border-top:1px solid var(--br);margin-top:4px;font-size:12px;font-weight:600"><span>Debería haber</span><span style="font-family:var(--mo);color:var(--ac)" id="cierre-deberia">${$m(fondoInicial+ef+(ingEf-egEf)-gaEf-(cierreHoy?.retiro||0))}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Queda en caja</span><span style="font-family:var(--mo)" id="cierre-contado-disp">$0</span></div>
      <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:12px;font-weight:700"><span>Diferencia</span><span style="font-family:var(--mo)" id="cierre-diferencia">—</span></div>
    </div>
    <button class="btn btnp" onclick="saveCierre()" style="width:100%">✓ Confirmar cierre</button>
    ${cierreHoy?`<div style="margin-top:8px;padding:8px;background:var(--sf2);border-radius:5px;font-size:11px;font-family:var(--mo);color:var(--tx2)">Último cierre: ${$m(cierreHoy.total_contado)} contado · Retiro: ${$m(cierreHoy.retiro)} · Fondo siguiente: <span style="color:var(--gn)">${$m(cierreHoy.saldo_siguiente)}</span>${cierreHoy.detalle?`<div style="margin-top:6px;display:flex;flex-wrap:wrap;gap:5px">${BILLETES.filter(b=>(cierreHoy.detalle[b]||0)>0).map(b=>`<span style="background:var(--sf);border-radius:4px;padding:2px 6px;font-size:10px">${cierreHoy.detalle[b]}× $${b.toLocaleString('es-AR')}</span>`).join('')}</div>`:''}</div>`:''}
  </div>

  <!-- CIERRE DIGITAL -->
  ${!S.cfg.cierreDigital?'':`<div class="blk" style="border-color:rgba(46,155,212,.3)">
    <div class="bt" style="color:var(--bl)">📱 Cierre digital — MP / Transferencia</div>
    <div style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin-bottom:8px">El fondo se arrastra solo día a día. Solo tocá el checkbox para corregirlo a mano.</div>
    <label style="display:flex;align-items:center;gap:6px;font-size:11px;margin-bottom:6px"><input type="checkbox" id="cierre-editar-fondo-dig" onchange="toggleFondoDigEdit()" style="width:auto"> Corregir fondo inicial digital manualmente</label>
    <div id="cierre-fondo-dig-manual-wrap" style="display:none;margin-bottom:8px"><input type="number" id="cierre-fondo-dig-manual" placeholder="Monto real" oninput="calcCierreDigital()"></div>
    <div style="background:var(--sf2);border-radius:8px;padding:10px;margin-bottom:8px;border:1px solid var(--br)">
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Fondo inicial digital</span><span style="font-family:var(--mo);color:var(--bl)" id="cierre-fondo-dig-cuenta">+${$m(fondoDigital)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Ventas digital</span><span style="font-family:var(--mo);color:var(--gn)">+${$m(tvCaja-ef)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Ingresos/egresos manuales (dig.)</span><span style="font-family:var(--mo);color:${(ingTr-egTr)>=0?'var(--gn)':'var(--rd)'}">${(ingTr-egTr)>=0?'+':''}${$m(ingTr-egTr)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:3px 0;font-size:11px"><span style="color:var(--tx2)">Gastos/compras digital</span><span style="font-family:var(--mo);color:var(--rd)">-${$m(gaTr)}</span></div>
      <div style="display:flex;justify-content:space-between;padding:5px 0 3px;border-top:1px solid var(--br);margin-top:4px;font-size:12px;font-weight:600"><span>Debería haber</span><span style="font-family:var(--mo);color:var(--bl)" id="cierre-deberia-dig">${$m(fondoDigital+(tvCaja-ef)+ingTr-egTr-gaTr)}</span></div>
    </div>
    <div class="fl" style="margin-bottom:8px"><label>Saldo real en MP ahora (copiá el número de la app)</label><input type="number" id="cierre-real-dig" placeholder="0" value="${cierreHoy?.saldo_digital_real||''}" oninput="calcCierreDigital()"></div>
    <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:12px;font-weight:700"><span>Diferencia</span><span style="font-family:var(--mo)" id="cierre-diferencia-dig">—</span></div>
    <button class="btn btnp" onclick="saveCierreDigital()" style="width:100%;margin-top:6px">✓ Confirmar cierre digital</button>
    ${cierreHoy?.saldo_digital_real!=null?`<div style="margin-top:8px;padding:8px;background:var(--sf2);border-radius:5px;font-size:11px;font-family:var(--mo);color:var(--tx2)">Último cierre digital: ${$m(cierreHoy.saldo_digital_real)} real · Fondo siguiente: <span style="color:var(--bl)">${$m(cierreHoy.saldo_digital_siguiente)}</span></div>`:`<div style="margin-top:8px;font-size:10px;color:var(--tx3);font-family:var(--mo)">Todavía no confirmaste el cierre digital de este día.</div>`}
  </div>`}`;
}

/* Ticket helpers */
function onTKVar(){const s=document.getElementById('tk-var'),o=s?.options[s.selectedIndex];if(!o)return;document.getElementById('tk-price').value=o.dataset.p||'';calcTK();}
function onTKCliente(v){
  if(v==='__nuevo'){
    const nombre=(prompt('Nombre del cliente nuevo:')||'').trim();
    if(!nombre){render();return;}
    const row={id:uid(),nombre,telefono:null,time:arTime()};
    S.ccl.push(row);save();sbUp('clientes_cc',row);
    v=row.id;toast('Cliente creado ✓');
  }
  const nuevo=v||null;
  if(nuevo!==tkClienteId&&ticketItems.length&&confirm(`¿Aplicar los precios de ${nuevo?nombreCliente(nuevo):'lista'} a los ítems que ya cargaste?`)){
    ticketItems.forEach(x=>{x.price=precioCliente(nuevo,x.varId);x.total=x.qty*x.price*(1-x.desc/100);});
  }
  tkClienteId=nuevo;render();
}
function calcTK(){
  const s=document.getElementById('tk-var'),o=s?.options[s.selectedIndex];
  const k=parseFloat(o?.dataset?.k)||1,q=parseFloat(document.getElementById('tk-qty')?.value)||0,p=parseFloat(document.getElementById('tk-price')?.value)||0,d=parseFloat(document.getElementById('tk-desc')?.value)||0;
  const sub=document.getElementById('tk-sub');if(sub)sub.value=q*p*(1-d/100)?$m(q*p*(1-d/100)):'';
}
// Pago mixto: se carga lo que entró por transferencia; el efectivo es el resto del total (el vuelto no importa)
function calcTKMixto(){
  const tr=parseFloat(document.getElementById('tk-pago-tr')?.value)||0;
  const tot=ticketItems.reduce((s,x)=>s+x.total,0);
  const el=document.getElementById('tk-pago-ef-calc');
  if(el)el.value=tr>0?(tr>=tot?'—':$m(tot-tr)):'';
}
function renderTKItems(){
  const list=document.getElementById('tk-items');if(!list)return;
  if(!ticketItems.length){list.innerHTML='';return;}
  const tot=ticketItems.reduce((s,x)=>s+x.total,0);
  list.innerHTML=`<div style="margin-top:8px;border-top:1px solid var(--br);padding-top:8px">`
    +ticketItems.map((x,i)=>`<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--br)">
      <div style="font-size:12px">${esc(x.nombre)} ×${x.qty}${x.desc>0?` <span class="tag tc">-${x.desc}%</span>`:''}</div>
      <div style="display:flex;align-items:center;gap:6px"><span style="font-family:var(--mo);font-size:12px;color:var(--tx)">${$m(x.total)}</span><button class="dbtn" onclick="rmTKItem(${i})">✕</button></div>
    </div>`).join('')
    +`<div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:600"><span style="font-size:12px">Subtotal</span><span style="font-family:var(--mo);font-size:14px;color:var(--tx)">${$m(tot)}</span></div></div>`;
}
function addTKItem(){
  const s=document.getElementById('tk-var'),o=s?.options[s.selectedIndex];
  const varId=s?.value,kpu=parseFloat(o?.dataset?.k)||1,qty=parseFloat(document.getElementById('tk-qty')?.value)||0;
  const price=parseFloat(document.getElementById('tk-price')?.value)||0,desc=parseFloat(document.getElementById('tk-desc')?.value)||0;
  if(!varId||!qty||!price)return alert('Completá producto y precio');
  const vr=S.vr.find(x=>x.id===varId),gr=S.sg.find(x=>x.id===vr?.group_id);
  const nombre=(gr?gr.name+' › ':'')+vr?.name;
  ticketItems.push({varId,groupId:vr?.group_id,nombre,qty,kpu,price,desc,total:qty*price*(1-desc/100),stockUsed:qty*kpu,unit:gr?.unit||'kg'});
  document.getElementById('tk-qty').value='1';document.getElementById('tk-desc').value='';
  render();
}
function rmTKItem(i){ticketItems.splice(i,1);render();}
function cancelarTicket(){if(!ticketItems.length)return;if(!confirm('¿Cancelar el ticket en proceso?'))return;ticketItems=[];pagoSeleccionado='Efectivo';tkClienteId=null;render();}
function selPago(tipo){
  pagoSeleccionado=tipo;
  const ef=document.getElementById('pago-btn-ef'),tr=document.getElementById('pago-btn-tr'),mix=document.getElementById('pago-btn-mix'),cc=document.getElementById('pago-btn-cc');
  const mixFields=document.getElementById('tk-mixto-fields');
  const mpField=document.getElementById('tk-mp-field');
  const ccField=document.getElementById('tk-cc-field');
  const resetBtn=b=>{if(b){b.style.borderColor='var(--br2)';b.style.background='var(--sf2)';b.style.color='var(--tx2)';}};
  [ef,tr,mix,cc].forEach(resetBtn);
  if(tipo==='Efectivo'&&ef){ef.style.borderColor='var(--gn)';ef.style.background='rgba(74,222,128,.12)';ef.style.color='var(--gn)';}
  else if(tipo==='Transferencia'&&tr){tr.style.borderColor='var(--bl)';tr.style.background='rgba(96,165,250,.12)';tr.style.color='var(--bl)';}
  else if(tipo==='mixto'&&mix){mix.style.borderColor='var(--ac)';mix.style.background='rgba(232,118,10,.12)';mix.style.color='var(--ac)';}
  else if(tipo==='cuenta_corriente'&&cc){cc.style.borderColor='var(--pu,#a78bfa)';cc.style.background='rgba(167,139,250,.12)';cc.style.color='var(--pu,#a78bfa)';}
  if(mixFields)mixFields.style.display=tipo==='mixto'?'block':'none';
  if(mpField)mpField.style.display=(tipo!=='Efectivo'&&tipo!=='cuenta_corriente')?'block':'none';
  if(ccField)ccField.style.display=tipo==='cuenta_corriente'?'block':'none';
}
function cerrarTicket(){
  if(!ticketItems.length)return alert('El ticket está vacío');
  const tot=ticketItems.reduce((s,x)=>s+x.total,0);
  let ef=0,tr=0,pago=pagoSeleccionado,clienteCcId=null;
  if(pagoSeleccionado==='mixto'){
    tr=parseFloat(document.getElementById('tk-pago-tr')?.value)||0;
    if(tr<=0)return alert('En pago mixto, cargá cuánto entró por transferencia. El resto se toma como efectivo.');
    if(tr>=tot)return alert(`La transferencia (${$m(tr)}) cubre todo el ticket (${$m(tot)}). Elegí "Transf." en vez de mixto.`);
    ef=tot-tr;
  }else if(pagoSeleccionado==='Efectivo'){ef=tot;}
  else if(pagoSeleccionado==='cuenta_corriente'){
    clienteCcId=tkClienteId;
    if(!clienteCcId)return alert('Para Cta.Cte. elegí el cliente arriba, en "Cliente".');
    // ef y tr quedan en 0: no entra plata real hoy, se suma al saldo del cliente
  }
  else{tr=tot;}
  if(day!==arDay()&&!tkHora&&!confirm(`Estás cargando una venta del ${fDL(day)} sin hora real. ¿Guardarla con la hora de ahora (${arTime()})?`))return;
  // comision medio digital
  let comision=0,mpLabel='';
  if(tr>0){
    const mpSel=document.getElementById('tk-mp-tipo');
    const mpOpt=mpSel?.options[mpSel.selectedIndex];
    const pct=parseFloat(mpOpt?.dataset?.pct)||0;
    mpLabel=mpOpt?.text||'';
    if(pct>0){comision=Math.round(tr*pct/100*100)/100;}
  }
  const tktId=uid(),time=(day!==arDay()&&tkHora)?fmtHora(tkHora):arTime();
  const rows=ticketItems.map(x=>{const g=S.sg.find(sg=>sg.id===x.groupId);return{id:uid(),day,ticket_id:tktId,variant_id:x.varId,group_id:x.groupId,qty:x.qty,stock_used:x.stockUsed,price_unit:x.price,descuento_pct:x.desc,total:x.total,costo_unit_venta:g?.cost_unit||0,pago,pago_ef:ef,pago_tr:tr,cliente_id:tkClienteId||null,cliente_cc_id:clienteCcId,cc_pagado:clienteCcId?false:null,usuario:sesion?.nombre||'—',time};});
  if(!S.ve[day])S.ve[day]=[];S.ve[day].push(...rows);
  // descontar stock
  ticketItems.forEach(x=>{const g=S.sg.find(sg=>sg.id===x.groupId);if(g)g.stock_qty=(g.stock_qty||0)-x.stockUsed;});
  sbUp('ventas',rows);
  subirGrupos(rows.map(r=>r.group_id).filter(Boolean));
  // registrar comision como gasto automatico
  if(comision>0){
    const gCom={id:uid(),day,descripcion:`Comisión ${mpLabel} (ticket ${time})`,cat:'Comisiones digitales',amount:comision,metodo:'transferencia',auto:true,time};
    if(!S.ga[day])S.ga[day]=[];S.ga[day].push(gCom);
    sbUp('gastos',gCom);
  }
  ticketItems=[];pagoSeleccionado='Efectivo';tkClienteId=null;tkHora='';save();render();toast('Ticket cerrado ✓'+(comision>0?` · Comisión ${$m(comision)} registrada`:''));
}
function delTicket(tid){
  if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar. Pedile que ingrese con su PIN para borrar esto.');
  const vs=S.ve[day]||[],items=vs.filter(v=>(v.ticket_id||v.id)===tid);
  const cobrado=items.some(v=>v.cliente_cc_id&&v.cc_pagado);
  if(!confirm(cobrado?'Este ticket de Cta.Cte. YA FUE COBRADO. Si lo borrás, el cobro queda cargado igual y el cliente va a quedar con saldo a favor. ¿Eliminar igual?':'¿Eliminar este ticket? Se revertirá el stock.'))return;
  items.forEach(v=>{const g=S.sg.find(x=>x.id===v.group_id);if(g)g.stock_qty=(g.stock_qty||0)+(v.stock_used||0);});
  S.ve[day]=vs.filter(v=>(v.ticket_id||v.id)!==tid);save();render();
  items.forEach(v=>sbDel('ventas',v.id));
  subirGrupos(items.map(v=>v.group_id).filter(Boolean));
}
/* Movimientos de caja */
function addMov(){
  const tipo=document.getElementById('mov-tipo').value,desc=document.getElementById('mov-desc').value.trim(),monto=parseFloat(document.getElementById('mov-monto').value)||0,metodo=document.getElementById('mov-metodo').value;
  if(!desc||!monto)return alert('Completá descripción y monto');
  const row={id:uid(),day,tipo,descripcion:desc,metodo,monto,usuario:sesion?.nombre||'—',time:arTime()};
  if(!S.caja[day])S.caja[day]=[];S.caja[day].push(row);save();render();
  sbUp('caja_movimientos',row);
}
function delMov(id){if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar. Pedile que ingrese con su PIN para borrar esto.');S.caja[day]=(S.caja[day]||[]).filter(x=>x.id!==id);save();render();sbDel('caja_movimientos',id);}

/* Cierre de caja */
function calcCierre(){
  let total=0;
  BILLETES.forEach(b=>{const q=parseInt(document.getElementById('bill_'+b)?.value)||0,sub=q*b;total+=sub;const el=document.getElementById('bsub_'+b);if(el)el.textContent=sub>0?$m(sub):'';});
  const retiro=parseFloat(document.getElementById('cierre-retiro')?.value)||0;
  const totEl=document.getElementById('cierre-total'),saldoEl=document.getElementById('cierre-saldo');
  if(totEl)totEl.textContent=$m(total);
  if(saldoEl)saldoEl.value=$m(Math.max(0,total-retiro));
  // actualizar cuenta del día
  const deberiaEl=document.getElementById('cierre-deberia'),contEl=document.getElementById('cierre-contado-disp'),difEl=document.getElementById('cierre-diferencia'),retDisp=document.getElementById('cierre-retiro-display');
  if(retDisp)retDisp.textContent='-'+$m(retiro);
  if(deberiaEl){
    const vs=dV();const{ef}=calcEfTr(vs);
    const compraGastoIdsC=idsGastoCompra();
    const gaEf=(S.ga[day]||[]).filter(g=>esGastoCaja(g,compraGastoIdsC)).reduce((s,g)=>s+gastoEf(g),0);
    const movs=dCaja();
    const ingEf=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
    const egEf=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
    const movCuenta=document.getElementById('cierre-mov-cuenta');
    if(movCuenta){const nm=ingEf-egEf;movCuenta.textContent=(nm>=0?'+':'')+$m(nm);movCuenta.style.color=nm>=0?'var(--gn)':'var(--rd)';}
    const prevDay=getPrevDay(day);
    const fondoBase=S.cierres?.[prevDay]?.saldo_siguiente||0;
    const cb=document.getElementById('cierre-editar-fondo');
    const fondoManualInp=document.getElementById('cierre-fondo-manual');
    const fondo=cb?.checked&&fondoManualInp?( parseFloat(fondoManualInp.value)||0 ):fondoBase;
    // actualizar la línea de fondo en la cuenta
    const fondoCuenta=document.getElementById('cierre-fondo-cuenta');
    if(fondoCuenta)fondoCuenta.textContent='+'+$m(fondo);
    const deberia=fondo+ef+(ingEf-egEf)-gaEf-retiro;
    deberiaEl.textContent=$m(deberia);
    if(contEl)contEl.textContent=$m(Math.max(0,total-retiro));
    if(difEl){const dif=Math.max(0,total-retiro)-deberia;difEl.textContent=(dif>=0?'+':'')+$m(dif);difEl.style.color=Math.abs(dif)<50?'var(--gn)':dif>0?'var(--ac)':'var(--rd)';}
  }
}
function toggleFondoEdit(){
  const cb=document.getElementById('cierre-editar-fondo');
  const inp=document.getElementById('cierre-fondo-manual');
  const disp=document.getElementById('cierre-fondo-display');
  if(inp)inp.style.display=cb?.checked?'block':'none';
  if(disp)disp.style.display=cb?.checked?'none':'inline';
  calcCierre();
}
function toggleFondoDigEdit(){
  const cb=document.getElementById('cierre-editar-fondo-dig'),wrap=document.getElementById('cierre-fondo-dig-manual-wrap');
  if(wrap)wrap.style.display=cb?.checked?'block':'none';
  calcCierreDigital();
}
function calcCierreDigital(){
  const cierrePrevDig=S.cierres?.[getPrevDay(day)];
  const cierreHoyDig=S.cierres?.[day];
  const cb=document.getElementById('cierre-editar-fondo-dig');
  const fondoManualInp=document.getElementById('cierre-fondo-dig-manual');
  const fondo=cb?.checked&&fondoManualInp?(parseFloat(fondoManualInp.value)||0):(cierreHoyDig?.fondo_digital_manual??cierrePrevDig?.saldo_digital_siguiente??0);
  const vs=dV().filter(v=>v.pago!=='cuenta_corriente'),tv=vs.reduce((s,v)=>s+v.total,0);const{ef}=calcEfTr(vs);
  const movs=dCaja();
  const ingTr=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='transferencia').reduce((s,m)=>s+m.monto,0);
  const egTr=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='transferencia').reduce((s,m)=>s+m.monto,0);
  const compraGastoIdsD=idsGastoCompra();
  const gaTr=(S.ga[day]||[]).filter(g=>esGastoCaja(g,compraGastoIdsD)).reduce((s,g)=>s+gastoTr(g),0);
  const deberia=fondo+(tv-ef)+ingTr-egTr-gaTr;
  const fondoCuenta=document.getElementById('cierre-fondo-dig-cuenta');if(fondoCuenta)fondoCuenta.textContent='+'+$m(fondo);
  const deberiaEl=document.getElementById('cierre-deberia-dig');if(deberiaEl)deberiaEl.textContent=$m(deberia);
  const real=parseFloat(document.getElementById('cierre-real-dig')?.value);
  const difEl=document.getElementById('cierre-diferencia-dig');
  if(difEl){
    if(isNaN(real)){difEl.textContent='—';difEl.style.color='var(--tx2)';}
    else{const dif=real-deberia;difEl.textContent=(dif>=0?'+':'')+$m(dif);difEl.style.color=Math.abs(dif)<50?'var(--gn)':dif>0?'var(--ac)':'var(--rd)';}
  }
}
function saveCierreDigital(){
  const real=parseFloat(document.getElementById('cierre-real-dig')?.value);
  if(isNaN(real))return alert('Ingresá el saldo real que ves en Mercado Pago');
  const cb=document.getElementById('cierre-editar-fondo-dig');
  const fondoManualInp=document.getElementById('cierre-fondo-dig-manual');
  const fondoManualVal=cb?.checked?parseFloat(fondoManualInp?.value)||0:undefined;
  if(!S.cierres)S.cierres={};
  const prev=S.cierres[day]||{total_contado:0,detalle:{},retiro:0,saldo_siguiente:0,fondo_inicial_manual:undefined,time:arTime()};
  S.cierres[day]={...prev,fondo_digital_manual:fondoManualVal,saldo_digital_real:real,saldo_digital_siguiente:real};
  save();render();toast('Cierre digital guardado ✓ — Fondo siguiente: '+$m(real));
  const c=S.cierres[day];
  sbUp('cierres',{id:'cierre_'+day,day,total_contado:c.total_contado,retiro:c.retiro,saldo_siguiente:c.saldo_siguiente,fondo_inicial_manual:c.fondo_inicial_manual??null,fondo_digital_manual:fondoManualVal??null,saldo_digital_real:real,saldo_digital_siguiente:real,detalle:parseDetalle(c.detalle),time:c.time});
}
function saveCierre(){
  let total=0;const detalle={};
  BILLETES.forEach(b=>{const q=parseInt(document.getElementById('bill_'+b)?.value)||0;detalle[b]=q;total+=q*b;});
  const retiro=parseFloat(document.getElementById('cierre-retiro')?.value)||0;
  const saldo=Math.max(0,total-retiro);
  if(total===0)return alert('Contá al menos un billete');
  const cb=document.getElementById('cierre-editar-fondo');
  const fondoManualVal=cb?.checked?parseFloat(document.getElementById('cierre-fondo-manual')?.value)||0:undefined;
  const fondoDigitalPrev=S.cierres?.[day]?.fondo_digital_manual;
  const saldoDigitalRealPrev=S.cierres?.[day]?.saldo_digital_real;
  const saldoDigitalSigPrev=S.cierres?.[day]?.saldo_digital_siguiente;
  if(!S.cierres)S.cierres={};
  S.cierres[day]={total_contado:total,detalle,retiro,saldo_siguiente:saldo,fondo_inicial_manual:fondoManualVal,fondo_digital_manual:fondoDigitalPrev,saldo_digital_real:saldoDigitalRealPrev,saldo_digital_siguiente:saldoDigitalSigPrev,time:arTime()};
  save();render();toast('Cierre guardado ✓ — Fondo siguiente: '+$m(saldo));
  sbUp('cierres',{id:'cierre_'+day,day,total_contado:total,retiro,saldo_siguiente:saldo,fondo_inicial_manual:fondoManualVal??null,fondo_digital_manual:fondoDigitalPrev??null,saldo_digital_real:saldoDigitalRealPrev??null,saldo_digital_siguiente:saldoDigitalSigPrev??null,detalle,time:arTime()});
  // Recordatorio: el cierre digital es el único control de lo que entra por transferencia
  if(S.cfg.cierreDigital&&saldoDigitalRealPrev==null)setTimeout(()=>alert('Efectivo cerrado ✓\n\nTe falta el CIERRE DIGITAL: abrí la app de Mercado Pago, copiá el saldo y cargalo en "Cierre digital" (abajo de todo).'),300);
}

/* Gastos */
function addGa(){const d2=document.getElementById('g-d').value.trim(),c=document.getElementById('g-c').value,a=parseFloat(document.getElementById('g-a').value)||0,met=document.getElementById('g-met')?.value||'efectivo';if(!d2||!a)return alert('Completá descripción y monto');const row={id:uid(),day,descripcion:d2,cat:c,amount:a,metodo:met,usuario:sesion?.nombre||'—',time:arTime()};if(!S.ga[day])S.ga[day]=[];S.ga[day].push(row);save();render();sbUp('gastos',row);}
// Borra un gasto de cualquier día (los del local se ven por mes, no solo el día actual)
function delGa(id){if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar. Pedile que ingrese con su PIN para borrar esto.');if(!confirm('¿Eliminar este gasto?'))return;Object.keys(S.ga).forEach(d=>{S.ga[d]=(S.ga[d]||[]).filter(x=>x.id!==id);});save();render();sbDel('gastos',id);}
function addGasCat(){const n=document.getElementById('gascat-n')?.value.trim();if(!n)return;if((S.cfg.gasCats||[]).includes(n))return alert('Ya existe');if(!S.cfg.gasCats)S.cfg.gasCats=[];S.cfg.gasCats.push(n);save();render();toast('Categoría agregada ✓');}
function delGasCat(i){if(!confirm('¿Eliminar esta categoría?'))return;S.cfg.gasCats.splice(i,1);save();render();}
function rGastos(){
  const _cg=idsGastoCompra();
  const gs=dG().filter(g=>esGastoCaja(g,_cg)),tot=gs.reduce((s,g)=>s+g.amount,0);
  const cats={};gs.forEach(g=>{cats[g.cat]=(cats[g.cat]||0)+g.amount});
  const cH=Object.entries(cats).sort((a,b)=>b[1]-a[1]).map(([c,v])=>`<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--br)"><span style="font-size:11px;color:var(--tx2)">${c}</span><span style="font-family:var(--mo);font-size:12px">${$m(v)}</span></div>`).join('');
  const catOpts=(S.cfg.gasCats||[]).map(c=>`<option>${esc(c)}</option>`).join('');
  const rows=gs.length?gs.map(g=>`<tr><td>${g.time||''}</td><td style="max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(g.descripcion)}</td><td><span class="tag tg">${(g.cat||'').slice(0,5)}</span></td><td>${$m(g.amount)}</td><td><button class="dbtn" onclick="delGa('${g.id}')">✕</button></td></tr>`).join(''):`<tr><td colspan="5" class="empty-row">Sin gastos</td></tr>`;
  return`<div class="kpis"><div class="kc hi"><div class="kl">Total gastos</div><div class="kv r">${$m(tot)}</div><div class="kh">${gs.length} registros</div></div></div>
  ${Object.keys(cats).length?`<div class="blk"><div class="bt">Por categoría</div>${cH}</div>`:''}
  <div class="blk"><div class="bt">Registrar gasto</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Descripción</label><input type="text" id="g-d" placeholder="Ej: Gas, bolsas, personal..."></div><div class="fl"><label>Categoría</label><select id="g-c">${catOpts}</select></div></div>
    <div class="fr"><div class="fl"><label>Monto $</label><input type="number" id="g-a" placeholder="0"></div><button class="btn btnp" onclick="addGa()" style="align-self:flex-end">+ Gasto</button></div>
  </div>
  <div class="tbk"><div class="tt">Gastos del día</div>
    <table><thead><tr><th>Hora</th><th>Descripción</th><th>Cat.</th><th>Monto</th><th></th></tr></thead><tbody>${rows}</tbody></table>
  </div>`;
}

/* ══ STOCK ══════════════════════════════════════════════════════ */
function rStock(){
  const low=sgV().filter(g=>(g.stock_qty||0)<2);
  const alrt=low.length?`<div class="alrt">⚠ Stock bajo: ${low.map(g=>esc(g.name)+' ('+fQ(g.stock_qty,g.unit)+')').join(', ')}</div>`:'';
  const uOpts=UNITS.map(u=>`<option>${u}</option>`).join('');
  const sgRows=tipo=>S.sg.filter(g=>(g.tipo||'venta')===tipo).map(g=>{
    const max=Math.max(...S.sg.filter(x=>(x.tipo||'venta')===tipo).map(x=>x.stock_qty||0),1),pct=Math.min(100,Math.round(((g.stock_qty||0)/max)*100));
    const col=(g.stock_qty||0)<2?'var(--rd)':(g.stock_qty||0)<5?'var(--or)':'var(--gn)';
    return`<tr><td><div style="font-size:12px">${esc(g.name)} <span style="font-size:9px;color:var(--tx3)">${g.unit||'kg'}</span></div>
      ${tipo==='venta'?`<div style="font-size:9px;color:var(--tx3)">${S.vr.filter(v=>v.group_id===g.id).map(v=>esc(v.name)).join(', ')||'sin variantes'}</div>
      <label style="display:flex;align-items:center;gap:4px;font-size:9px;font-family:var(--mo);color:${g.subproducto?'var(--ac)':'var(--tx3)'};margin-top:2px;cursor:pointer"><input type="checkbox" ${g.subproducto?'checked':''} onchange="setSubproducto('${g.id}',this.checked)" style="width:auto;margin:0"> subproducto</label>`:''}
      <div class="sb-w"><div class="sb" style="width:${pct}%;background:${col}"></div></div></td>
      <td style="color:${col};font-family:var(--mo);font-weight:500">${fQ(g.stock_qty,g.unit)}</td>
      <td><input type="number" class="ip" value="${+(g.stock_qty||0).toFixed(3)}" step="0.1" onchange="updGS('${g.id}',this.value)"></td>
      <td><input type="number" class="ip" value="${Math.round(g.cost_unit||0)}" step="1" min="0" onchange="updGCost('${g.id}',this.value)">${infoCosto(g)}</td>
      <td><button class="dbtn" onclick="delG('${g.id}')">✕</button></td>
    </tr>`;
  }).join('')||`<tr><td colspan="5" class="empty-row">Sin grupos</td></tr>`;
  const vrRows=sgV().map(g=>{const vars=S.vr.filter(v=>v.group_id===g.id);if(!vars.length)return'';
    return`<tr style="background:rgba(255,255,255,.01)"><td colspan="4" style="padding:5px 12px;font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:.5px;border-top:1px solid var(--br)">${esc(g.name)} (${g.unit||'kg'})</td></tr>`
      +vars.map(v=>`<tr><td style="padding-left:18px">${esc(v.name)}</td><td style="color:var(--tx3);font-size:10px">−${v.qty_per_unit} ${g.unit||'kg'}/u</td><td><input type="number" class="ip" value="${v.price||0}" onchange="updVP('${v.id}',this.value)"></td><td><button class="dbtn" onclick="delVr('${v.id}')">✕</button></td></tr>`).join('');
  }).join('');
  const grOptV=sgV().map(g=>`<option value="${g.id}">${esc(g.name)} (${g.unit||'kg'})</option>`).join('');
  return`${alrt}
  <div class="sh">Stock de venta</div>
  <div class="blk"><div class="bt">Nuevo grupo de venta</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Nombre</label><input type="text" id="sg-n" placeholder="Ej: Cuartos, Supremas, Milanesas..."></div><div class="fl" style="max-width:90px"><label>Unidad</label><select id="sg-u">${uOpts}</select></div><div class="fl" style="max-width:70px"><label>Stock inicial</label><input type="number" id="sg-s" placeholder="0" step="0.1"></div></div>
    <button class="btn btnp" onclick="addG('venta')" style="width:100%;margin-top:4px">+ Crear grupo</button>
  </div>
  <div class="info-box">💲 El <b>costo</b> se calcula solo: promedio de los lotes (compras, cortes y elaboraciones) de los últimos ${COSTO_VENTANA_DIAS} días, ponderado por kg. No depende del stock. Si lo corregís a mano, el promedio vuelve a arrancar desde hoy (sirve para el conteo de inicio de mes).</div>
  ${sesion?.rol==='dueno'?`<button class="btn" onclick="recalcularTodos()" style="width:100%;margin-bottom:10px">↻ Recalcular todos los costos con los lotes</button>`:''}
  <div class="tbk"><div class="tt">Stock de venta</div><div class="tbk-hint">→ deslizá para ver el costo</div><div class="tbk-scroll">
    <table><thead><tr><th>Grupo</th><th>Stock</th><th>Ajustar stock</th><th>Costo $/u</th><th></th></tr></thead><tbody>${sgRows('venta')}</tbody></table>
  </div></div>
  <div class="sh">Variantes de venta</div>
  <div class="blk"><div class="bt">Nueva variante</div>
    <div class="fr"><div class="fl"><label>Grupo</label><select id="vr-g">${grOptV||'<option>Sin grupos</option>'}</select></div><div class="fl" style="flex:2"><label>Nombre</label><input type="text" id="vr-n" placeholder="Ej: Cuarto x kg, Oferta 3kg, Pata..."></div></div>
    <div class="fr"><div class="fl"><label>Cantidad que descuenta</label><input type="number" id="vr-k" placeholder="1" min="0.001" step="0.001"></div><div class="fl"><label>Precio $</label><input type="number" id="vr-p" placeholder="0"></div><button class="btn btnp" onclick="addVr()" style="align-self:flex-end">+ Crear</button></div>
  </div>
  <div class="tbk"><div class="tt">Variantes — editá precio directo</div>
    <table><thead><tr><th>Variante</th><th>Descuenta</th><th>Precio $</th><th></th></tr></thead><tbody>${vrRows||`<tr><td colspan="4" class="empty-row">Sin variantes</td></tr>`}</tbody></table>
  </div>
  ${sesion?.rol==='dueno'?`
  <div class="sh">Este dispositivo</div>
  <div class="blk">
    <div style="font-size:11px;color:var(--tx2);font-family:var(--mo);margin-bottom:8px">Vinculado con la cuenta <b>${esc(auGet()?.email||'—')}</b></div>
    <button class="btn" onclick="desvincularDispositivo()" style="width:100%;color:var(--rd)">Cerrar sesión de este dispositivo</button>
  </div>
  <div class="sh">Cierre digital (Mercado Pago)</div>
  <div class="blk">
    <label style="display:flex;align-items:center;gap:8px;font-size:12px"><input type="checkbox" ${S.cfg.cierreDigital?'checked':''} onchange="setCierreDigital(this.checked)" style="width:auto"> Usar cierre digital al final del día</label>
    <div style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin-top:6px">Apagado: no pide el saldo de MP ni muestra diferencias (para cuando usás MP para gastos personales). Encendelo cuando MP sea solo del local, por ejemplo con empleada. Es por dispositivo.</div>
  </div>
  <div class="sh">Categorías de gastos operativos</div>
  <div class="blk">
    <div style="font-size:10px;color:var(--tx2);font-family:var(--mo);margin-bottom:10px">Estas categorías aparecen al cargar un gasto operativo.</div>
    ${(S.cfg.gasCats||[]).map((c,i)=>`<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid var(--br)"><span style="font-size:12px">${esc(c)}</span><button class="dbtn" onclick="delGasCat(${i})">✕</button></div>`).join('')||'<div style="font-size:11px;color:var(--tx3);padding:4px 0">Sin categorías</div>'}
    <div class="fr" style="margin-top:10px">
      <div class="fl" style="flex:2"><label>Nueva categoría</label><input type="text" id="gascat-n" placeholder="Ej: Mantenimiento, Publicidad..."></div>
      <button class="btn btnp" onclick="addGasCat()" style="align-self:flex-end">+ Agregar</button>
    </div>
  </div>
  <div class="sh">Configuración de comisiones digitales</div>
  <div class="blk">
    <div style="font-size:10px;color:var(--tx2);font-family:var(--mo);margin-bottom:10px">Los porcentajes se aplican automáticamente al monto en transferencia de cada ticket.</div>
    <table style="width:100%"><thead><tr><th>Medio</th><th>% comisión</th><th></th></tr></thead><tbody>
      ${(S.cfg.mp||[]).map((m,i)=>`<tr>
        <td style="font-size:11px">${esc(m.label)}</td>
        <td><input type="number" class="ip" value="${m.pct}" step="0.01" min="0" max="100" onchange="updMPPct(${i},this.value)" style="width:60px"></td>
        <td><button class="dbtn" onclick="delMPItem(${i})">✕</button></td>
      </tr>`).join('')}
    </tbody></table>
    <div class="fr" style="margin-top:10px">
      <div class="fl" style="flex:2"><label>Nuevo medio</label><input type="text" id="mp-label" placeholder="Ej: MODO, Naranja X..."></div>
      <div class="fl" style="max-width:80px"><label>% comisión</label><input type="number" id="mp-pct" placeholder="3.5" step="0.01" min="0"></div>
      <button class="btn btnp" onclick="addMPItem()" style="align-self:flex-end">+ Agregar</button>
    </div>
  </div>
  <div class="sh">Usuarios y acceso</div>
  <div class="blk">
    <div style="font-size:10px;color:var(--tx2);font-family:var(--mo);margin-bottom:10px">Editá nombre y PIN de cada usuario. El PIN debe tener entre 4 y 6 dígitos.</div>
    ${S.us.filter(u=>u.id!=='usr_dueno'&&u.id!=='usr_empleado'||true).map((u,i)=>`
    <div style="display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid var(--br)">
      <div style="flex:2"><input type="text" class="ip" value="${esc(u.nombre)}" placeholder="Nombre" style="width:100%" onchange="updUsrNombre('${u.id}',this.value)"></div>
      <div style="flex:1"><input type="text" class="ip" value="${u.pin||''}" placeholder="PIN" maxlength="6" style="width:100%;letter-spacing:3px" onchange="updUsrPin('${u.id}',this.value)"></div>
      <span style="font-size:9px;font-family:var(--mo);color:var(--tx3);white-space:nowrap">${u.rol==='dueno'?'👑 dueño':'👤 empleado'}</span>
    </div>`).join('')||'<div style="font-size:11px;color:var(--tx3)">Sin usuarios</div>'}
    <div class="fr" style="margin-top:12px">
      <div class="fl"><label>Nombre</label><input type="text" id="usr-n" placeholder="Nombre"></div>
      <div class="fl" style="max-width:90px"><label>PIN (4-6 dígitos)</label><input type="text" id="usr-pin" placeholder="0000" maxlength="6"></div>
      <div class="fl" style="max-width:90px"><label>Rol</label><select id="usr-rol"><option value="empleado">Empleado</option><option value="dueno">Dueño</option></select></div>
      <button class="btn btnp" onclick="addUsr()" style="align-self:flex-end">+ Agregar</button>
    </div>
  </div>
  <div class="sh">Copia de seguridad</div>
  <div class="blk">
    <div style="font-size:10px;color:var(--tx2);font-family:var(--mo);margin-bottom:8px">Baja todos los datos de la nube a un archivo. Hacelo una vez por semana y guardalo fuera del celular (Drive o mail). Último backup desde este dispositivo: <b style="color:${LC.g('ultimo_backup')?'var(--gn)':'var(--rd)'}">${LC.g('ultimo_backup')?fDL(LC.g('ultimo_backup')):'nunca'}</b></div>
    <button class="btn btnp" onclick="exportBackup()" style="width:100%">⬇ Descargar backup completo</button>
  </div>`:''}`;
}
// Detalle de cómo se llegó al costo (debajo del input de costo en Stock)
function infoCosto(g){
  if(g.subproducto)return`<div style="font-size:8px;color:var(--ac);font-family:var(--mo);margin-top:2px">subproducto: $0</div>`;
  const W=lotesVigentes(g);
  if(!W.length)return`<div style="font-size:8px;color:var(--tx3);font-family:var(--mo);margin-top:2px">${g.cost_manual_at?'a mano el '+fD(String(g.cost_manual_at).slice(0,10)):'sin lotes'}</div>`;
  const kg=W.reduce((s,l)=>s+l.qty,0);
  return`<div style="font-size:8px;color:var(--tx3);font-family:var(--mo);margin-top:2px" title="${esc(W.map(l=>fD(l.day)+' '+l.src+': '+fQ(l.qty,'kg')+' a '+$m(l.cu)).join('\n'))}">${W.length} lote${W.length>1?'s':''} · ${fQ(kg,'kg')}</div>`;
}
function addG(tipo){
  const n=document.getElementById(tipo==='produccion'?'sgp-n':'sg-n')?.value.trim(),u=document.getElementById(tipo==='produccion'?'sgp-u':'sg-u')?.value||'kg',s=parseFloat(document.getElementById(tipo==='produccion'?'sgp-s':'sg-s')?.value)||0;
  if(!n)return alert('Ingresá un nombre');if(S.sg.find(g=>g.name===n&&(g.tipo||'venta')===tipo))return alert('Ya existe');
  const row={id:uid(),name:n,unit:u,stock_qty:s,tipo,cost_unit:0,cost_manual_at:null};S.sg.push(row);save();render();
  sbUp('stock_groups',sgRow(row));
}
// Recalcula el costo de todos los grupos con la regla de lotes y muestra qué cambió
function recalcularTodos(){
  const cambios=[];
  sgV().forEach(g=>{const antes=g.cost_unit||0;recalcCosto(g);if(Math.abs((g.cost_unit||0)-antes)>=1)cambios.push({g,antes});});
  if(!cambios.length)return toast('Todos los costos ya están al día ✓');
  const txt=cambios.map(c=>`• ${c.g.name}: ${$m(c.antes)} → ${$m(c.g.cost_unit)}`).join('\n');
  if(!confirm('Estos costos cambian con el promedio de lotes:\n\n'+txt+'\n\n¿Guardarlos?')){cambios.forEach(c=>c.g.cost_unit=c.antes);return;}
  save();render();subirGrupos(cambios.map(c=>c.g.id));toast(`${cambios.length} costo(s) actualizados ✓`);
}
// Subproducto (carcaza, menudos...): costo $0; el costo del cajón lo cargan los cortes principales
function setSubproducto(id,on){
  const g=S.sg.find(x=>x.id===id);if(!g)return;
  if(!confirm(on?`¿Marcar "${g.name}" como SUBPRODUCTO?\n\n• Su costo pasa a $0.\n• En los próximos cortes, el costo del cajón se reparte solo entre los cortes principales.\n• Lo cargás igual en el corte, con sus kg reales.`:`¿Sacar la marca de subproducto a "${g.name}"?\n\nEn los próximos cortes va a cargar costo como un corte principal.`)){render();return;}
  g.subproducto=on;
  if(on){g.cost_unit=0;g.cost_manual_at=new Date().toISOString();}
  else{g.cost_manual_at=null;recalcCosto(g);} // vuelve a tomar el costo de sus lotes (compras/cortes)
  save();render();sbUp('stock_groups',sgRow(g));toast(on?`${g.name}: subproducto ✓`:`${g.name}: corte principal ✓`);
}
function updGS(id,v){const g=S.sg.find(x=>x.id===id);if(!g)return;g.stock_qty=parseFloat(v)||0;save();toast('Stock actualizado ✓');sbUp('stock_groups',sgRow(g));}
// Costo a mano: queda ese valor y el promedio de lotes vuelve a arrancar desde hoy
function updGCost(id,v){const g=S.sg.find(x=>x.id===id);if(!g)return;g.cost_unit=parseFloat(v)||0;g.cost_manual_at=new Date().toISOString();save();render();toast('Costo actualizado ✓ — el promedio arranca desde hoy');sbUp('stock_groups',sgRow(g));}
function delG(id){if(!confirm('¿Eliminar grupo?'))return;S.sg=S.sg.filter(x=>x.id!==id);S.vr=S.vr.filter(x=>x.group_id!==id);save();render();sbDel('stock_groups',id);}
function addVr(){const gid=document.getElementById('vr-g').value,n=document.getElementById('vr-n').value.trim(),k=parseFloat(document.getElementById('vr-k').value)||0,p=parseFloat(document.getElementById('vr-p').value)||0;if(!gid||!n||!k)return alert('Completá todos los campos');const row={id:uid(),group_id:gid,name:n,qty_per_unit:k,price:p};S.vr.push(row);save();render();sbUp('stock_variants',row);}
function updVP(id,v){const vr=S.vr.find(x=>x.id===id);if(!vr)return;vr.price=parseFloat(v)||0;save();toast('Precio actualizado ✓');sbUp('stock_variants',{id:vr.id,group_id:vr.group_id,name:vr.name,qty_per_unit:vr.qty_per_unit,price:vr.price});}
function delVr(id){if(!confirm('¿Eliminar esta variante?'))return;S.vr=S.vr.filter(x=>x.id!==id);S.cp=S.cp.filter(p=>p.variant_id!==id);save();render();sbDel('stock_variants',id);}

/* Gestión de usuarios */
function updUsrNombre(id,v){
  const u=S.us.find(x=>x.id===id);if(!u)return;
  const nombre=v.trim();if(!nombre)return;
  u.nombre=nombre;save();toast('Nombre actualizado ✓');
  // Actualizar sesión activa si es el usuario logueado
  if(sesion&&sesion.id===id){sesion.nombre=nombre;SC.s('sesion',sesion);document.getElementById('hdr-sub').textContent='Hoy · '+fDL(day)+' · '+nombre;}
  sbUp('usuarios',{id:u.id,nombre:u.nombre,pin:u.pin,rol:u.rol});
}
function updUsrPin(id,v){
  const u=S.us.find(x=>x.id===id);if(!u)return;
  const pin=v.trim();
  if(!/^\d{4,6}$/.test(pin))return alert('El PIN debe tener entre 4 y 6 dígitos numéricos');
  u.pin=pin;save();toast('PIN actualizado ✓');
  sbUp('usuarios',{id:u.id,nombre:u.nombre,pin:u.pin,rol:u.rol});
}
function addUsr(){
  const n=document.getElementById('usr-n')?.value.trim(),pin=document.getElementById('usr-pin')?.value.trim(),rol=document.getElementById('usr-rol')?.value||'empleado';
  if(!n)return alert('Ingresá un nombre');
  if(!/^\d{4,6}$/.test(pin))return alert('El PIN debe tener entre 4 y 6 dígitos numéricos');
  if(S.us.find(u=>u.pin===pin&&u.rol===rol))return alert('Ya existe un usuario con ese PIN y rol');
  const row={id:uid(),nombre:n,pin,rol,activo:true};
  S.us.push(row);save();render();toast('Usuario agregado ✓');
  sbUp('usuarios',row);
}

function setCierreDigital(on){
  S.cfg.cierreDigital=!!on;save();render();
  if(on)alert('Cierre digital activado.\n\nEl primer día, en el cierre digital tildá "Corregir fondo inicial digital" y poné el saldo real de MP de esa mañana: el fondo que trae arrastrado es del último cierre que hiciste y va a estar viejo.');
  else toast('Cierre digital apagado ✓');
}
function updMPPct(i,v){if(!S.cfg.mp[i])return;S.cfg.mp[i].pct=parseFloat(v)||0;save();toast('Comisión actualizada ✓');}
function delMPItem(i){if(!confirm('¿Eliminar este medio?'))return;S.cfg.mp.splice(i,1);save();render();}
function addMPItem(){const lbl=document.getElementById('mp-label')?.value.trim(),pct=parseFloat(document.getElementById('mp-pct')?.value)||0;if(!lbl)return alert('Ingresá el nombre del medio');S.cfg.mp.push({id:uid(),label:lbl,pct});save();render();toast('Medio agregado ✓');}

/* ══ PRODUCCIÓN ══════════════════════════════════════════════════════ */
function rProd(){
  return`<div class="prod-tabs">
    <button class="prod-tab ${prodTab==='corte'?'active':''}" onclick="setProdTab('corte')">✂ Corte — Ingreso stock</button>
    <button class="prod-tab ${prodTab==='elab'?'active':''}" onclick="setProdTab('elab')">🍳 Elaboración</button>
    <button class="prod-tab ${prodTab==='retiros'?'active':''}" onclick="setProdTab('retiros')">🏠 Retiros</button>
  </div>
  ${prodTab==='corte'?rCorte():prodTab==='elab'?rElab():rRetiros()}`;
}
function setProdTab(t){prodTab=t;render();}

function lotesMpcPendientes(){return S.coi.filter(x=>x.tipo_destino==='materia_prima_cruda'&&!x.usado);}
// Rinde de un corte: kg cortados sobre kg del lote (cajón)
/* Corte de un cajón:
   - Los cortes PRINCIPALES (cuartos, supremas, alas, entero...) cargan todo el costo del cajón:
     costo/kg = precio del cajón ÷ kg de cortes principales. Así la merma y los subproductos
     ya quedan pagados dentro del costo de los principales.
   - Los SUBPRODUCTOS (carcaza, menudos...) entran al stock con costo $0.
   - Merma = kg del cajón − (principales + subproductos): agua, sangre, recortes.            */
function esSub(gid){return!!S.sg.find(g=>g.id===gid)?.subproducto;}
function desgloseCorte(loteKg,items){
  const kgOf=arr=>arr.filter(x=>(x.unit||'kg')==='kg').reduce((s,x)=>s+(+x.qty||0),0);
  const princ=kgOf(items.filter(x=>!esSub(x.group_id))),sub=kgOf(items.filter(x=>esSub(x.group_id)));
  return{princ,sub,merma:loteKg-princ-sub,r:loteKg>0&&(princ+sub)>0?(princ+sub)/loteKg:null};
}
function rindeCorte(loteKg,items){return desgloseCorte(loteKg,items).r;}
function costoKgPrincipal(lote,items){const d=desgloseCorte(+lote.qty_real||+lote.qty_compra||0,items);return d.princ>0?lote.precio_total/d.princ:0;}
function rindeCorteTxt(r,loteKg,items,lote){
  if(r==null)return'';
  const d=desgloseCorte(loteKg,items);
  const mal=r>1.02||r<0.85;
  const pct=x=>loteKg>0?Math.round(x/loteKg*100)+'%':'';
  const ckg=lote&&d.princ>0?lote.precio_total/d.princ:0;
  return`<div style="font-size:10px;font-family:var(--mo);padding:4px 0;line-height:1.6">
    <div style="color:var(--tx2)">Principales: <b>${fQ(d.princ,'kg')}</b> (${pct(d.princ)}) · Subproductos: <b>${fQ(d.sub,'kg')}</b> (${pct(d.sub)}) · <span style="color:${mal?'var(--rd)':'var(--tx2)'}">Merma: <b>${fQ(d.merma,'kg')}</b> (${pct(d.merma)})</span></div>
    ${ckg?`<div style="color:var(--ac)">Costo de los principales: <b>${$m(ckg)}/kg</b> · subproductos $0</div>`:''}
    ${mal?`<div style="color:var(--rd)">⚠ ${r>1.02?'Sale más de lo que entró, revisá los pesos':'Merma alta, revisá los pesos'}</div>`:''}
  </div>`;
}
function rCorte(){
  const todC=S.ct.filter(c=>c.day===day);
  const sgVOpts=sgV().map(g=>`<option value="${g.id}" data-u="${g.unit||'kg'}">${esc(g.name)} (${g.unit||'kg'})${g.subproducto?' · subproducto':''}</option>`).join('');
  const lotes=lotesMpcPendientes();
  const loteOpts=lotes.map(l=>{const kg=l.qty_real||l.qty_compra,ckg=l.precio_total/kg,c=S.co.find(x=>x.id===l.compra_id);return`<option value="${l.id}" data-ckg="${ckg}" data-kg="${kg}">${c?fD(c.day)+' · ':''}${esc(l.descripcion)} — ${fQ(kg,'kg')} — ${$d2(ckg)}/kg</option>`}).join('');
  const cards=todC.length?todC.map(c=>{
    const items=S.cti.filter(i=>i.corte_id===c.id);
    const lote=c.origen_compra_item_id?S.coi.find(x=>x.id===c.origen_compra_item_id):null;
    const loteKg=lote?(+lote.qty_real||+lote.qty_compra||0):0;
    return`<div class="lote-card"><div class="lote-card-header"><div><div style="font-size:13px;font-weight:600">${esc(c.nombre)}</div><div style="font-size:10px;color:var(--tx3);font-family:var(--mo)">${c.time||''}${c.origen_compra_item_id?' · con costeo':' · sin costeo'}</div></div><button class="dbtn" onclick="delCorte('${c.id}')">✕</button></div>${lote?rindeCorteTxt(rindeCorte(loteKg,items),loteKg,items,lote):''}${items.length?`<div class="lote-card-items">${items.map(i=>`<div style="font-size:10px;color:var(--tx2);padding:2px 0">+ ${fQ(i.qty,i.unit)} → ${esc(i.nombre)}${lote?(i.cost_unit_aplicado?' — '+$m(i.cost_unit_aplicado)+'/kg':' — subproducto $0'):''}</div>`).join('')}</div>`:''}</div>`;}).join(''):`<div class="empty-row">Sin cortes hoy</div>`;
  return`<div class="info-box green">✂ Elegí el cajón del que sale este trozado y cargá <b>todo</b> lo que salió, pesado: cortes principales y también carcaza y menudos. El costo del cajón lo cargan solo los <b>principales</b>; los <b>subproductos</b> (marcados en Stock) entran a $0. Lo que falta para llegar a los kg del cajón es merma.</div>
  <div class="blk"><div class="bt">Nuevo corte</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Nombre</label><input type="text" id="ct-n" placeholder="Ej: Corte mañana, Tanda 1..."></div><div class="fl"><label>Nota</label><input type="text" id="ct-note" placeholder="opcional"></div></div>
    <div class="fr"><div class="fl" style="flex:2"><label>Lote de materia prima (cajón)</label><select id="ct-lote" onchange="onCtLote()"><option value="">Sin costeo (no hay factura de cajón)</option>${loteOpts}</select></div></div>
    <div id="ct-lote-info" style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin-top:2px"></div>
  </div>
  <div class="blk"><div class="bt">Cortes obtenidos</div>
    <div id="corte-items-list"></div>
    <div class="fr" style="margin-top:8px"><div class="fl" style="flex:2"><label>Grupo de stock</label><select id="ci-grp">${sgVOpts||'<option>Sin grupos</option>'}</select></div><div class="fl" style="max-width:70px"><label>Cantidad</label><input type="number" id="ci-qty" placeholder="0" min="0.01" step="0.01"></div><button class="btn" onclick="addCorteItem()" style="align-self:flex-end;padding:6px 10px;font-size:11px">+</button></div>
    <button class="btn btnp" onclick="saveCorte()" style="width:100%;margin-top:8px">✓ Guardar corte</button>
  </div>
  <div class="sh">Cortes de hoy</div>${cards}`;
}
function loteSeleccionado(){const id=document.getElementById('ct-lote')?.value;return id?S.coi.find(x=>x.id===id):null;}
function onCtLote(){const sel=document.getElementById('ct-lote'),opt=sel?.options[sel.selectedIndex],info=document.getElementById('ct-lote-info');if(!info)return;if(sel?.value&&opt){info.textContent=`Costo del lote: ${$d2(parseFloat(opt.dataset.ckg))}/kg sobre ${fQ(parseFloat(opt.dataset.kg),'kg')} comprados`;}else{info.textContent='';}renderCorteItems();}
function renderCorteItems(){
  const list=document.getElementById('corte-items-list');if(!list)return;
  if(!corteItems.length){list.innerHTML=`<div style="font-size:11px;color:var(--tx3);font-family:var(--mo);padding:4px 0">Sin cortes agregados</div>`;return;}
  const lote=loteSeleccionado(),loteKg=lote?(+lote.qty_real||+lote.qty_compra||0):0;
  list.innerHTML=corteItems.map((x,i)=>`<div class="pvi"><div><div class="pvn">${esc(x.nombre)} <span class="tag tv">+${fQ(x.qty,x.unit)}</span>${esSub(x.group_id)?' <span class="tag to">subproducto</span>':''}</div></div><button class="dbtn" onclick="rmCorteItem(${i})">✕</button></div>`).join('')
    +(lote?rindeCorteTxt(rindeCorte(loteKg,corteItems),loteKg,corteItems,lote):'');
}
function addCorteItem(){const sel=document.getElementById('ci-grp'),opt=sel?.options[sel.selectedIndex],gid=sel?.value,qty=parseFloat(document.getElementById('ci-qty')?.value)||0;if(!gid||!qty)return alert('Seleccioná grupo y cantidad');const g=S.sg.find(x=>x.id===gid);corteItems.push({group_id:gid,nombre:g?g.name:gid,qty,unit:opt?.dataset?.u||g?.unit||'kg'});document.getElementById('ci-qty').value='';renderCorteItems();}
function rmCorteItem(i){corteItems.splice(i,1);renderCorteItems();}
function saveCorte(){
  const nom=document.getElementById('ct-n')?.value.trim(),note=document.getElementById('ct-note')?.value.trim();
  const loteId=document.getElementById('ct-lote')?.value||null;
  if(!nom)return alert('Ingresá un nombre');if(!corteItems.length)return alert('Agregá al menos un corte');
  const lote=loteId?S.coi.find(x=>x.id===loteId):null;
  const loteKg=lote?(+lote.qty_real||+lote.qty_compra||0):0;
  const costoKg=lote?costoKgPrincipal(lote,corteItems):0;
  if(lote&&!(costoKg>0))return alert('Cargá al menos un corte principal (no subproducto): son los que cargan el costo del cajón.');
  const r=lote?rindeCorte(loteKg,corteItems):null;
  if(r!=null&&(r>1.02||r<0.85)&&!confirm(`El corte da ${Math.round(r*100)}% del lote (${fQ(loteKg,'kg')}). ${r>1?'Sale más de lo que entró.':'La merma es alta.'}\n\n¿Los pesos están bien? Aceptar = guardar igual.`))return;
  const cId=uid(),corte={id:cId,day,nombre:nom,note:note||null,origen_compra_item_id:loteId||null,usuario:sesion?.nombre||'—',time:arTime()};
  const items=corteItems.map(x=>({id:uid(),corte_id:cId,group_id:x.group_id,nombre:x.nombre,qty:x.qty,unit:x.unit,cost_unit_aplicado:lote&&!esSub(x.group_id)?costoKg:0}));
  S.ct.push(corte);S.cti.push(...items);
  items.forEach(x=>{const g=S.sg.find(sg=>sg.id===x.group_id);if(g)g.stock_qty=(g.stock_qty||0)+x.qty;});
  if(lote){lote.usado=true;items.forEach(x=>{const g=S.sg.find(sg=>sg.id===x.group_id);if(g)recalcCosto(g);});}
  corteItems=[];save();render();
  sbUp('cortes',corte);
  if(items.length)sbUp('cortes_items',items);
  if(lote)sbUp('compras_items',{id:lote.id,compra_id:lote.compra_id,descripcion:lote.descripcion,tipo_destino:lote.tipo_destino,ref_id:lote.ref_id,qty_compra:lote.qty_compra,unit_compra:lote.unit_compra,qty_real:lote.qty_real,unit_real:lote.unit_real,precio_total:lote.precio_total,cost_unit_calculado:lote.cost_unit_calculado,usado:true});
  subirGrupos(items.map(x=>x.group_id));
}
function delCorte(id){
  if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar. Pedile que ingrese con su PIN para borrar esto.');
  if(!confirm('¿Eliminar? Se revertirá el stock y se recalcula el costo sin este corte.'))return;
  const corte=S.ct.find(x=>x.id===id);
  const items=S.cti.filter(x=>x.corte_id===id);
  items.forEach(x=>{const g=S.sg.find(sg=>sg.id===x.group_id);if(g)g.stock_qty=(g.stock_qty||0)-x.qty;});
  const lote=corte?.origen_compra_item_id?S.coi.find(x=>x.id===corte.origen_compra_item_id):null;
  if(lote)lote.usado=false;
  S.ct=S.ct.filter(x=>x.id!==id);S.cti=S.cti.filter(x=>x.corte_id!==id);
  items.forEach(x=>{const g=S.sg.find(sg=>sg.id===x.group_id);if(g)recalcCosto(g);});
  save();render();
  sbDel('cortes',id); // los ítems se borran solos en la base (cascade)
  if(lote)sbUp('compras_items',{id:lote.id,compra_id:lote.compra_id,descripcion:lote.descripcion,tipo_destino:lote.tipo_destino,ref_id:lote.ref_id,qty_compra:lote.qty_compra,unit_compra:lote.unit_compra,qty_real:lote.qty_real,unit_real:lote.unit_real,precio_total:lote.precio_total,cost_unit_calculado:lote.cost_unit_calculado,usado:false});
  subirGrupos(items.map(x=>x.group_id));
}

function rindeElabTxt(r,prom){
  if(r==null)return'';
  const dev=prom?Math.abs(r-prom)/prom:0,mal=prom&&dev>RINDE_TOLERANCIA;
  return`<div style="font-size:10px;font-family:var(--mo);color:${mal?'var(--rd)':'var(--gn)'};padding:3px 0">${mal?'⚠ ':''}Rinde: ${r.toFixed(2)} kg por kg de materia prima${prom?` · promedio anterior ${prom.toFixed(2)}`:''}${mal?' — fuera de lo normal, revisá el peso':''}</div>`;
}
function rElab(){
  const todE=S.el.filter(e=>e.day===day);
  const sgVOpts=sgV().map(g=>`<option value="${g.id}">${esc(g.name)}</option>`).join('');
  const allStOpts=sgV().map(g=>`<option value="sg_${g.id}" data-cu="${g.cost_unit||0}" data-u="${g.unit||'kg'}">${esc(g.name)} (costo: ${$d2(g.cost_unit||0)}/${g.unit||'kg'})</option>`).join('');
  const cards=todE.length?todE.map(e=>{const outG=S.sg.find(x=>x.id===e.output_group_id),items=S.eli.filter(i=>i.elaboracion_id===e.id),costoKg=e.output_qty>0?(e.costo_total_info||0)/e.output_qty:0;return`<div class="lote-card"><div class="lote-card-header"><div><div style="font-size:13px;font-weight:600">${esc(e.nombre)}</div><div style="font-size:10px;color:var(--tx3);font-family:var(--mo)">${e.time||''}${outG?' → '+outG.name+' +'+fQ(e.output_qty,outG.unit):''}</div></div><div style="text-align:right"><div style="font-size:11px;font-family:var(--mo);color:var(--tx2)">Costo ref.: ${$m(e.costo_total_info||0)}</div>${outG?`<div style="font-size:10px;font-family:var(--mo);color:var(--ac)">${$m(costoKg)}/${outG.unit}</div>`:''}<button class="dbtn" onclick="delElab('${e.id}')" style="margin-top:3px">✕</button></div></div>${rindeElabTxt(rindeElab(e.output_qty,items),rindePromedio(e.output_group_id,e.id))}${items.length?`<div class="lote-card-items">${items.map(i=>`<div style="font-size:10px;color:var(--tx2);padding:2px 0">−${fQ(i.qty,i.unit)} ${esc(i.nombre)} — ref. ${$m(i.costo_subtotal||0)}</div>`).join('')}</div>`:''}</div>`;}).join(''):`<div class="empty-row">Sin elaboraciones hoy</div>`;
  return`<div class="info-box amber">🍳 Los costos son <strong>solo referenciales</strong>. No generan gastos — ya están en las facturas de compra. <b>Pesá siempre el producto terminado</b>: el rinde define el costo por kg.</div>
  <div class="blk"><div class="bt">Nuevo lote de elaboración</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Nombre</label><input type="text" id="el-n" placeholder="Ej: Milanesas tarde..."></div><div class="fl"><label>Nota</label><input type="text" id="el-note" placeholder=""></div></div>
    <div class="fr"><div class="fl" style="flex:2"><label>Resultado → ingresa a stock de venta</label><select id="el-outg" onchange="renderElabItems()"><option value="">No ingresa</option>${sgVOpts}</select></div><div class="fl" style="max-width:80px"><label>Kg reales</label><input type="number" id="el-outqty" placeholder="0" step="0.01" oninput="renderElabItems()"></div></div>
  </div>
  <div class="blk"><div class="bt">Ingredientes (descuenta stock)</div>
    <div id="elab-items-list"></div>
    <div class="fr" style="margin-top:8px">
      <div class="fl" style="max-width:100px"><label>Tipo</label><select id="eli-tipo" onchange="onElabTipo()"><option value="stock">Stock venta</option><option value="insumo">Insumo</option></select></div>
      <div class="fl" style="flex:2"><label>Item</label><select id="eli-item">${allStOpts||'<option>Sin grupos</option>'}</select></div>
      <div class="fl" style="max-width:65px"><label>Cant.</label><input type="number" id="eli-qty" placeholder="0" min="0.001" step="0.001"></div>
      <button class="btn" onclick="addElabItem()" style="align-self:flex-end;padding:6px 10px;font-size:11px">+</button>
    </div>
    <button class="btn btnp" onclick="saveElab()" style="width:100%;margin-top:8px">✓ Guardar elaboración</button>
  </div>
  <div class="sh">Elaboraciones de hoy</div>${cards}
  <div class="sh">Insumos</div>${rInsumosBlk()}`;
}
function onElabTipo(){const t=document.getElementById('eli-tipo')?.value,s=document.getElementById('eli-item');if(!s)return;if(t==='stock')s.innerHTML=sgV().map(g=>`<option value="sg_${g.id}" data-cu="${g.cost_unit||0}" data-u="${g.unit||'kg'}">${esc(g.name)} (costo: ${$d2(g.cost_unit||0)}/${g.unit||'kg'})</option>`).join('')||'<option>Sin grupos</option>';else s.innerHTML=S.ins.map(i=>`<option value="ins_${i.id}" data-cu="${i.costUnit||0}" data-u="${i.unit}">${esc(i.name)} (${$d2(i.costUnit||0)}/${i.unit})</option>`).join('')||'<option>Sin insumos</option>';}
function renderElabItems(){
  const list=document.getElementById('elab-items-list');if(!list)return;
  if(!elabItems.length){list.innerHTML=`<div style="font-size:11px;color:var(--tx3);font-family:var(--mo);padding:4px 0">Sin ingredientes</div>`;return;}
  const cT=elabItems.reduce((s,x)=>s+x.costo_subtotal,0);
  const outQty=parseFloat(document.getElementById('el-outqty')?.value)||0,outGid=document.getElementById('el-outg')?.value;
  list.innerHTML=elabItems.map((x,i)=>`<div class="pvi"><div><div class="pvn">${esc(x.nombre)} <span class="tag ${x.tipo==='stock'?'tp':'tc'}">${x.tipo}</span></div><div class="pvd">−${fQ(x.qty,x.unit)} × ${$d2(x.costo_unit)} = ${$m(x.costo_subtotal)} <span style="color:var(--tx3)">(ref.)</span></div></div><button class="dbtn" onclick="rmElabItem(${i})">✕</button></div>`).join('')
    +`<div style="text-align:right;font-size:12px;font-family:var(--mo);color:var(--tx2);padding:6px 0;border-top:1px solid var(--br);margin-top:4px">Costo ref. total: ${$m(cT)}${outQty>0?` · ${$m(cT/outQty)}/kg`:''}</div>`
    +(outGid?rindeElabTxt(rindeElab(outQty,elabItems),rindePromedio(outGid,null)):'');
}
function addElabItem(){
  const tipo=document.getElementById('eli-tipo')?.value,sel=document.getElementById('eli-item'),opt=sel?.options[sel.selectedIndex],val=sel?.value,qty=parseFloat(document.getElementById('eli-qty')?.value)||0;
  if(!val||!qty)return alert('Seleccioná item y cantidad');
  const cu=parseFloat(opt?.dataset?.cu)||0,unit=opt?.dataset?.u||'kg';
  let nombre='',ref_id='';
  if(tipo==='stock'){ref_id=val.replace('sg_','');const g=S.sg.find(x=>x.id===ref_id);nombre=g?g.name:ref_id;}
  else{ref_id=val.replace('ins_','');const i=S.ins.find(x=>x.id===ref_id);nombre=i?i.name:ref_id;}
  elabItems.push({tipo,ref_id,nombre,qty,unit,costo_unit:cu,costo_subtotal:qty*cu});
  document.getElementById('eli-qty').value='';renderElabItems();
}
function rmElabItem(i){elabItems.splice(i,1);renderElabItems();}
function saveElab(){
  const nom=document.getElementById('el-n')?.value.trim(),note=document.getElementById('el-note')?.value.trim(),outGid=document.getElementById('el-outg')?.value,outQty=parseFloat(document.getElementById('el-outqty')?.value)||0;
  if(!nom)return alert('Ingresá un nombre');if(!elabItems.length)return alert('Agregá al menos un ingrediente');
  if(outGid&&!(outQty>0))return alert('Cargá los kg reales que salieron (pesados).');
  const r=outGid?rindeElab(outQty,elabItems):null,prom=outGid?rindePromedio(outGid,null):null;
  if(r&&prom&&Math.abs(r-prom)/prom>RINDE_TOLERANCIA&&!confirm(`El rinde da ${r.toFixed(2)} kg por kg de materia prima y el promedio de las anteriores es ${prom.toFixed(2)}.\n\n¿Pesaste el producto terminado? Aceptar = guardar igual.`))return;
  const costoInfo=elabItems.reduce((s,x)=>s+x.costo_subtotal,0);
  const eId=uid(),elab={id:eId,day,nombre:nom,output_group_id:outGid||null,output_qty:outQty,costo_total_info:costoInfo,note:note||null,usuario:sesion?.nombre||'—',time:arTime()};
  const items=elabItems.map(x=>({id:uid(),elaboracion_id:eId,tipo:x.tipo,ref_id:x.ref_id,nombre:x.nombre,qty:x.qty,unit:x.unit,costo_unit:x.costo_unit,costo_subtotal:x.costo_subtotal}));
  items.forEach(x=>{
    if(x.tipo==='stock'){const g=S.sg.find(sg=>sg.id===x.ref_id);if(g)g.stock_qty=(g.stock_qty||0)-x.qty;}
    else if(x.tipo==='insumo'){const ins=S.ins.find(i=>i.id===x.ref_id);if(ins)ins.stock_qty=(ins.stock_qty||0)-x.qty;}
  });
  S.el.push(elab);S.eli.push(...items);
  if(outGid&&outQty>0){const og=S.sg.find(x=>x.id===outGid);if(og){og.stock_qty=(og.stock_qty||0)+outQty;recalcCosto(og);}}
  elabItems=[];save();render();
  sbUp('elaboraciones',elab);
  if(items.length)sbUp('elaboraciones_items',items);
  subirGrupos([...(outGid?[outGid]:[]),...items.filter(x=>x.tipo==='stock').map(x=>x.ref_id)]);
  subirInsumos(items.filter(x=>x.tipo==='insumo').map(x=>x.ref_id));
}
function delElab(id){
  if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar. Pedile que ingrese con su PIN para borrar esto.');
  if(!confirm('¿Eliminar? Se revertirá el stock y se recalcula el costo sin esta elaboración.'))return;
  const elab=S.el.find(x=>x.id===id);if(!elab)return;
  const items=S.eli.filter(x=>x.elaboracion_id===id);
  items.forEach(x=>{if(x.tipo==='stock'){const g=S.sg.find(sg=>sg.id===x.ref_id);if(g)g.stock_qty=(g.stock_qty||0)+x.qty;}else if(x.tipo==='insumo'){const ins=S.ins.find(i=>i.id===x.ref_id);if(ins)ins.stock_qty=(ins.stock_qty||0)+x.qty;}});
  S.el=S.el.filter(x=>x.id!==id);S.eli=S.eli.filter(x=>x.elaboracion_id!==id);
  if(elab.output_group_id&&elab.output_qty){const og=S.sg.find(x=>x.id===elab.output_group_id);if(og){og.stock_qty=(og.stock_qty||0)-elab.output_qty;recalcCosto(og);}}
  save();render();
  sbDel('elaboraciones',id); // los ingredientes se borran solos en la base (cascade)
  subirGrupos([...(elab.output_group_id?[elab.output_group_id]:[]),...items.filter(x=>x.tipo==='stock').map(x=>x.ref_id)]);
  subirInsumos(items.filter(x=>x.tipo==='insumo').map(x=>x.ref_id));
}

function rInsumosBlk(){
  const rows=S.ins.length?S.ins.map(i=>`<tr><td>${esc(i.name)}</td><td><input type="number" class="ip" step="0.01" value="${+(+(i.stock_qty||0)).toFixed(3)}" onchange="updInsStock('${i.id}',this.value)" style="max-width:75px"></td><td><input type="number" class="ip" value="${i.costUnit||0}" onchange="updIns('${i.id}',this.value)"></td><td><button class="dbtn" onclick="delIns('${i.id}')">✕</button></td></tr>`).join(''):`<tr><td colspan="4" class="empty-row">Sin insumos</td></tr>`;
  return`<div class="blk"><div class="bt">Agregar insumo</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Nombre</label><input type="text" id="ins-n" placeholder="Ej: Pan rallado, Rebozador, Aceite..."></div><div class="fl" style="max-width:75px"><label>Unidad</label><select id="ins-u"><option>kg</option><option>litro</option><option>unidad</option><option>bolsa</option></select></div></div>
    <div class="fr"><div class="fl"><label>Costo/unidad $</label><input type="number" id="ins-c" placeholder="0"></div><button class="btn btnp" onclick="addIns()" style="align-self:flex-end">+ Agregar</button></div>
    <div style="font-size:9px;color:var(--tx3);font-family:var(--mo);margin-top:3px">Stock entra desde Compras · Sale desde Elaboración · También lo podés corregir a mano acá abajo</div>
  </div>
  <div class="tbk"><div class="tt">Insumos — stock y costo</div>
    <table><thead><tr><th>Insumo</th><th>Stock</th><th>Costo $</th><th></th></tr></thead><tbody>${rows}</tbody></table>
  </div>`;
}
function addIns(){const n=document.getElementById('ins-n')?.value.trim(),u=document.getElementById('ins-u')?.value,c=parseFloat(document.getElementById('ins-c')?.value)||0;if(!n||!c)return alert('Completá nombre y costo');if(S.ins.find(i=>i.name===n))return alert('Ya existe');const row={id:uid(),name:n,unit:u,costUnit:c,cost_unit:c,stock_qty:0};S.ins.push(row);save();render();sbUp('insumos',insRow(row));}
function updInsStock(id,v){const i=S.ins.find(x=>x.id===id);if(!i)return;i.stock_qty=parseFloat(v)||0;save();toast('Stock corregido ✓');sbUp('insumos',insRow(i));}
function updIns(id,v){const i=S.ins.find(x=>x.id===id);if(!i)return;i.costUnit=parseFloat(v)||0;i.cost_unit=i.costUnit;save();toast('Costo actualizado ✓');sbUp('insumos',insRow(i));}
function delIns(id){if(!confirm('¿Eliminar este insumo?'))return;S.ins=S.ins.filter(x=>x.id!==id);save();render();sbDel('insumos',id);}

/* ══ RETIROS EN MERCADERÍA (consumo personal del dueño) ═══════════
   Baja stock y NO toca caja, ventas ni Resultado del local.
   Se muestran aparte para saber cuánto representa lo que se lleva el dueño.
   El costo ya está dentro de las compras del mes: por eso no se resta de nuevo. */
function retirosPeriodo(matchDay){
  const L=(S.rt||[]).filter(r=>matchDay(String(r.day).slice(0,10)));
  const porG={};
  L.forEach(r=>{const g=S.sg.find(x=>x.id===r.group_id),n=g?.name||r.nombre||'—';if(!porG[n])porG[n]={qty:0,unit:r.unit||g?.unit||'kg',costo:0,venta:0};porG[n].qty+=+r.stock_used||0;porG[n].costo+=+r.costo_total||0;porG[n].venta+=+r.valor_venta||0;});
  return{L,n:L.length,costo:L.reduce((s,r)=>s+(+r.costo_total||0),0),venta:L.reduce((s,r)=>s+(+r.valor_venta||0),0),porG};
}
// Bloque para Reportes (Mes y Financiero): informativo, no modifica ningún número del local
function blkRetiros(ym,resultado){
  const R=retirosPeriodo(d=>d.startsWith(ym));
  if(!R.n)return'';
  const filas=Object.entries(R.porG).sort((a,b)=>b[1].costo-a[1].costo).map(([n,d])=>`<tr><td>${esc(n)}</td><td style="font-family:var(--mo)">${fQ(d.qty,d.unit)}</td><td style="font-family:var(--mo)">${$m(d.costo)}</td><td style="font-family:var(--mo);color:var(--tx2)">${$m(d.venta)}</td></tr>`).join('');
  const pct=resultado>0?` · ${Math.round(R.costo/resultado*100)}% del resultado`:'';
  return`<div class="blk"><div class="bt">🏠 Mis retiros en mercadería — ${fM(ym)}</div>
    <div class="kpis t3" style="margin-bottom:6px"><div class="kc"><div class="kl">A costo</div><div class="kv" style="font-size:14px">${$m(R.costo)}</div></div><div class="kc"><div class="kl">A precio de venta</div><div class="kv" style="font-size:14px;color:var(--tx2)">${$m(R.venta)}</div></div><div class="kc"><div class="kl">Retiros</div><div class="kv" style="font-size:14px">${R.n}</div></div></div>
    <div style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin-bottom:6px">Aparte: no cambia el Resultado del local (el costo ya está en las compras)${pct}</div>
    <table><thead><tr><th>Producto</th><th>Cant.</th><th>Costo</th><th>Venta</th></tr></thead><tbody>${filas}</tbody></table></div>`;
}
function rRetiros(){
  const ym=day.slice(0,7);
  const gIds=new Set(sgV().map(g=>g.id));
  const opts=S.vr.filter(v=>gIds.has(v.group_id)).map(v=>{const g=S.sg.find(x=>x.id===v.group_id);return{v,g,lbl:`${g.name} — ${v.name}`};}).sort((a,b)=>a.lbl.localeCompare(b.lbl))
    .map(({v,g,lbl})=>`<option value="${v.id}">${esc(lbl)} (${$m(v.price)} · costo ${$m((g.cost_unit||0)*(v.qty_per_unit||1))})</option>`).join('');
  const hoy=(S.rt||[]).filter(r=>String(r.day).slice(0,10)===day).sort((a,b)=>String(a.created_at||a.time||'').localeCompare(String(b.created_at||b.time||'')));
  const cards=hoy.map(r=>`<div class="pvi"><div><div class="pvn">${esc(r.nombre)}</div><div class="pvd">${r.time||''} · −${fQ(r.stock_used,r.unit)} · costo ${$m(r.costo_total)} · venta ${$m(r.valor_venta)}${r.note?' · '+esc(r.note):''}</div></div><button class="dbtn" onclick="delRetiro('${r.id}')">✕</button></div>`).join('')||`<div style="font-size:11px;color:var(--tx3);font-family:var(--mo);padding:4px 0">Sin retiros este día</div>`;
  const M=retirosPeriodo(d=>d.startsWith(ym));
  return`<div class="info-box amber">🏠 Lo que te llevás para tu casa. <b>Baja el stock</b> pero <b>no toca la caja, las ventas ni el Resultado</b> del local. Lo ves aparte en Reportes.</div>
  <div class="blk"><div class="bt">Nuevo retiro</div>
    <div class="fr"><div class="fl" style="flex:3"><label>Producto</label><select id="rt-var">${opts||'<option value="">Sin variantes</option>'}</select></div><div class="fl" style="max-width:80px"><label>Cant.</label><input type="number" id="rt-qty" placeholder="0" min="0.001" step="0.001"></div></div>
    <div class="fr"><div class="fl"><label>Nota (opcional)</label><input type="text" id="rt-note" placeholder="Ej: cena domingo"></div></div>
    <button class="btn btnp" onclick="saveRetiro()" style="width:100%;margin-top:8px">✓ Guardar retiro</button>
  </div>
  <div class="sh">Retiros del día</div>${cards}
  <div class="sh">Acumulado ${fM(ym)}</div>
  <div class="kpis t3"><div class="kc"><div class="kl">A costo</div><div class="kv" style="font-size:14px">${$m(M.costo)}</div></div><div class="kc"><div class="kl">A precio de venta</div><div class="kv" style="font-size:14px;color:var(--tx2)">${$m(M.venta)}</div></div><div class="kc"><div class="kl">Retiros</div><div class="kv" style="font-size:14px">${M.n}</div></div></div>`;
}
function saveRetiro(){
  const vid=document.getElementById('rt-var')?.value,qty=parseFloat(document.getElementById('rt-qty')?.value)||0,note=document.getElementById('rt-note')?.value.trim();
  const vr=S.vr.find(x=>x.id===vid),g=vr&&S.sg.find(x=>x.id===vr.group_id);
  if(!vr||!g)return alert('Elegí un producto');
  if(!(qty>0))return alert('Cargá la cantidad');
  const stockUsed=qty*(+vr.qty_per_unit||1),cu=+g.cost_unit||0;
  const row={id:uid(),day,group_id:g.id,variant_id:vr.id,nombre:`${g.name} — ${vr.name}`,qty,stock_used:stockUsed,unit:g.unit||'kg',costo_unit:cu,costo_total:stockUsed*cu,valor_venta:qty*(+vr.price||0),motivo:'consumo_personal',note:note||null,usuario:sesion?.nombre||'—',time:arTime()};
  g.stock_qty=(g.stock_qty||0)-stockUsed;
  S.rt.push(row);save();render();
  sbUp('retiros_stock',row);subirGrupos([g.id]);
  toast('Retiro guardado ✓');
}
function delRetiro(id){
  if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar. Pedile que ingrese con su PIN para borrar esto.');
  const r=S.rt.find(x=>x.id===id);if(!r)return;
  if(!confirm('¿Eliminar este retiro? El stock vuelve a sumar.'))return;
  const g=S.sg.find(x=>x.id===r.group_id);if(g)g.stock_qty=(g.stock_qty||0)+(+r.stock_used||0);
  S.rt=S.rt.filter(x=>x.id!==id);save();render();
  sbDel('retiros_stock',id);if(g)subirGrupos([g.id]);
}

/* ══ COMPRAS ══════════════════════════════════════════════════════ */
/* ══ CLIENTES Y CUENTAS CORRIENTES ═══════════════════════════════
   Cualquier venta puede llevar cliente (contado o Cta.Cte.).
   El saldo de Cta.Cte. sale de: ventas a Cta.Cte. − pagos registrados. */
function saldoClienteCC(id){
  let ventas=0,pagos=0;
  Object.values(S.ve).forEach(arr=>arr.forEach(v=>{if(v.cliente_cc_id===id)ventas+=v.total;}));
  Object.values(S.caja).forEach(arr=>arr.forEach(m=>{if(m.cliente_cc_id===id&&m.tipo==='ingreso')pagos+=m.monto;}));
  return ventas-pagos;
}
// Todas las compras del cliente (contado y Cta.Cte.), agrupadas por ticket
function ticketsCliente(id){
  const tickets={};
  Object.entries(S.ve).forEach(([d,arr])=>arr.forEach(v=>{
    if(v.cliente_id!==id&&v.cliente_cc_id!==id)return;
    const tk=v.ticket_id||v.id;
    if(!tickets[tk])tickets[tk]={tipo:'venta',day:d,time:v.time,created_at:v.created_at,items:[],total:0,kg:0,ticketId:tk,cc:!!v.cliente_cc_id,pagado:true,pago:v.pago};
    tickets[tk].items.push(v);tickets[tk].total+=v.total;tickets[tk].kg+=(v.stock_used||0);
    if(v.cliente_cc_id&&!v.cc_pagado)tickets[tk].pagado=false;
  }));
  return Object.values(tickets);
}
function resumenCliente(id){
  const tks=ticketsCliente(id),desde=sumarDias(arDay(),-30);
  const ult=tks.reduce((m,t)=>t.day>m?t.day:m,'');
  const t30=tks.filter(t=>t.day>=desde);
  const dias=ult?Math.round((new Date(arDay()+'T12:00:00')-new Date(ult+'T12:00:00'))/86400000):null;
  return{ult,dias,compras:tks.length,kg30:t30.reduce((s,t)=>s+t.kg,0),monto30:t30.reduce((s,t)=>s+t.total,0)};
}
function movimientosClienteCC(id){
  const pagos=[];
  Object.entries(S.caja).forEach(([d,arr])=>arr.forEach(m=>{
    if(m.cliente_cc_id===id&&m.tipo==='ingreso')pagos.push({tipo:'pago',day:d,time:m.time,created_at:m.created_at,monto:m.monto,metodo:m.metodo,id:m.id,descripcion:m.descripcion});
  }));
  const all=[...ticketsCliente(id),...pagos];
  all.sort((a,b)=>{const ka=a.day+String(timeToMin(a.time)).padStart(5,'0'),kb=b.day+String(timeToMin(b.time)).padStart(5,'0');return ka<kb?1:ka>kb?-1:0;});
  return all;
}
// Tickets de Cta.Cte. de este cliente, de cualquier mes, que todavía no están pagados
function ticketsPendientesCC(id){
  return ticketsCliente(id).filter(t=>t.cc&&!t.pagado).sort((a,b)=>(a.day+(a.time||'')).localeCompare(b.day+(b.time||'')));
}
function rCC(){
  if(ccClienteId)return rCCDetail(ccClienteId);
  const clientes=[...S.ccl].sort((a,b)=>a.nombre.localeCompare(b.nombre));
  const rows=clientes.map(c=>{
    const saldo=saldoClienteCC(c.id),r=resumenCliente(c.id);
    const ultTxt=r.dias==null?'sin compras':r.dias===0?'compró hoy':`última compra hace ${r.dias} día${r.dias===1?'':'s'}`;
    return`<div class="lote-card" style="cursor:pointer" onclick="verClienteCC('${c.id}')">
      <div class="lote-card-header">
        <div><div style="font-size:14px;font-weight:600">${esc(c.nombre)}</div>
          <div style="font-size:10px;color:${r.dias!=null&&r.dias>7?'var(--rd)':'var(--tx3)'};font-family:var(--mo)">${ultTxt}${r.kg30>0?` · ${fQ(r.kg30,'kg')} en 30 días`:''}</div>
          ${c.telefono?`<div style="font-size:10px;color:var(--tx3);font-family:var(--mo)">${esc(c.telefono)}</div>`:''}</div>
        <div style="text-align:right">${Math.abs(saldo)>=1?`<div style="font-size:16px;font-weight:700;font-family:var(--mo);color:${saldo>0?'var(--rd)':'var(--gn)'}">${$m(saldo)}</div><div style="font-size:9px;color:var(--tx3);font-family:var(--mo)">${saldo>0?'nos debe':'a favor'}</div>`:`<div style="font-size:9px;color:var(--tx3);font-family:var(--mo)">al día</div>`}</div>
      </div>
    </div>`;
  }).join('')||`<div class="empty-row">Sin clientes todavía</div>`;
  return`<div class="info-box">👥 Clientes: podés ponerle cliente a cualquier venta desde Caja (contado o Cta.Cte.). Cada cliente puede tener <b>precios especiales</b>, y ves cuándo compró por última vez.</div>
  <div class="blk"><div class="bt">Nuevo cliente</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Nombre</label><input type="text" id="cc-nombre" placeholder="Ej: Distribuidora Sur"></div><div class="fl"><label>Teléfono / nota</label><input type="text" id="cc-tel" placeholder="opcional"></div></div>
    <button class="btn btnp" onclick="saveClienteCC()" style="width:100%;margin-top:6px">+ Crear cliente</button>
  </div>
  <div class="sh">Clientes</div>${rows}`;
}
function saveClienteCC(){
  const nombre=document.getElementById('cc-nombre')?.value.trim(),tel=document.getElementById('cc-tel')?.value.trim();
  if(!nombre)return alert('Ingresá el nombre del cliente');
  if(S.ccl.some(c=>c.nombre.toLowerCase()===nombre.toLowerCase())&&!confirm('Ya hay un cliente con ese nombre. ¿Crear otro igual?'))return;
  const row={id:uid(),nombre,telefono:tel||null,time:arTime()};
  S.ccl.push(row);save();render();toast('Cliente creado ✓');
  sbUp('clientes_cc',row);
}
function updCliente(id,campo,v){
  const c=S.ccl.find(x=>x.id===id);if(!c)return;
  v=(v||'').trim();if(campo==='nombre'&&!v)return alert('El nombre no puede quedar vacío');
  c[campo]=v||null;save();toast('Cliente actualizado ✓');
  sbUp('clientes_cc',{id:c.id,nombre:c.nombre,telefono:c.telefono||null,time:c.time||null});
}
function delClienteCC(id){
  if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar clientes.');
  const saldo=saldoClienteCC(id);
  if(Math.abs(saldo)>=1){if(!confirm(`Este cliente todavía tiene un saldo de ${$m(saldo)}. ¿Eliminar igual? Las ventas y pagos ya cargados NO se borran, solo se elimina el cliente de la lista.`))return;}
  else if(!confirm('¿Eliminar este cliente de la lista? Sus ventas quedan registradas.'))return;
  S.ccl=S.ccl.filter(c=>c.id!==id);S.cp=S.cp.filter(p=>p.cliente_id!==id);ccClienteId=null;save();render();
  sbDel('clientes_cc',id); // sus precios especiales se borran solos en la base (cascade)
}
// Precio especial por cliente: vacío = vuelve al precio de lista
function setPrecioCliente(clienteId,varId,v){
  const id='cp_'+clienteId+'_'+varId,precio=parseFloat(v);
  if(!(precio>0)){
    if(S.cp.some(p=>p.id===id)){S.cp=S.cp.filter(p=>p.id!==id);save();sbDel('cliente_precios',id);toast('Vuelve al precio de lista');}
    return;
  }
  const row={id,cliente_id:clienteId,variant_id:varId,price:precio};
  const i=S.cp.findIndex(p=>p.id===id);if(i>=0)S.cp[i]=row;else S.cp.push(row);
  save();sbUp('cliente_precios',row);toast('Precio especial guardado ✓');
}
function verClienteCC(id){ccClienteId=id;ccTicketAbierto=null;ccSeleccionados=new Set();render();}
function volverCCList(){ccClienteId=null;ccSeleccionados=new Set();render();}
function toggleCCTicket(tid){ccTicketAbierto=ccTicketAbierto===tid?null:tid;render();}
function itemsTicketHtml(items){return items.map(v=>{const vr=S.vr.find(x=>x.id===v.variant_id),gr=S.sg.find(x=>x.id===v.group_id);return`<div style="font-size:10px;color:var(--tx2);padding:2px 0">${esc(gr?gr.name:'–')}${vr?' › '+esc(vr.name):''} ×${v.qty} = ${$m(v.total)}</div>`;}).join('');}
function rCCDetail(id){
  const c=S.ccl.find(x=>x.id===id);
  if(!c){ccClienteId=null;return rCC();}
  const saldo=saldoClienteCC(id),r=resumenCliente(id);
  const movs=movimientosClienteCC(id);
  const pendientes=ticketsPendientesCC(id);
  const movRows=movs.length?movs.map(m=>{
    if(m.tipo==='pago'){
      return`<div class="lote-card" style="border-left:3px solid var(--gn)">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <div><div style="font-size:12px;font-weight:600;color:var(--gn)">✓ Pago recibido</div><div style="font-size:10px;color:var(--tx3);font-family:var(--mo)">${fDL(m.day)} · ${m.time||''} · ${m.metodo==='transferencia'?'Transferencia':'Efectivo'}${m.descripcion?` · ${esc(m.descripcion)}`:''}</div></div>
          <div style="font-size:14px;font-weight:700;font-family:var(--mo);color:var(--gn)">-${$m(m.monto)}</div>
        </div>
      </div>`;
    }
    const abierto=ccTicketAbierto===m.ticketId;
    const estado=m.cc?(m.pagado?'<span style="color:var(--gn);font-weight:400">Cta.Cte. ✓ pagado</span>':'<span style="color:var(--rd);font-weight:400">Cta.Cte. pendiente</span>'):'<span style="color:var(--tx3);font-weight:400">contado</span>';
    return`<div class="lote-card" style="border-left:3px solid ${m.cc&&!m.pagado?'var(--rd)':'var(--tx3)'};cursor:pointer" onclick="toggleCCTicket('${m.ticketId}')">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div><div style="font-size:12px;font-weight:600">Compra · ${estado}</div><div style="font-size:10px;color:var(--tx3);font-family:var(--mo)">${fDL(m.day)} · ${m.time||''} · ${fQ(m.kg,'kg')}</div></div>
        <div style="font-size:14px;font-weight:700;font-family:var(--mo);color:${m.cc&&!m.pagado?'var(--rd)':'var(--tx2)'}">${m.cc?'+':''}${$m(m.total)}</div>
      </div>
      ${abierto?`<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--br)">${itemsTicketHtml(m.items)}</div>`:`<div style="font-size:9px;color:var(--tx3);font-family:var(--mo);margin-top:4px">Tocá para ver el detalle</div>`}
    </div>`;
  }).join(''):`<div class="empty-row">Sin movimientos todavía</div>`;
  const pendRows=pendientes.length?pendientes.map(t=>{
    const abierto=ccTicketAbierto===t.ticketId;
    return`<div style="background:var(--sf2);border-radius:7px;margin-bottom:5px;padding:8px 10px">
    <div style="display:flex;justify-content:space-between;align-items:center;cursor:pointer" onclick="toggleCCTicket('${t.ticketId}')">
      <span style="display:flex;align-items:center;gap:8px"><input type="checkbox" class="cc-tk-check" data-tk="${t.ticketId}" data-monto="${t.total}" ${ccSeleccionados.has(t.ticketId)?'checked':''} onclick="event.stopPropagation()" onchange="toggleCCSeleccion('${t.ticketId}',this.checked)" style="width:auto"><span style="font-size:11px;font-family:var(--mo)">${fDL(t.day)} · ${t.time||''}</span></span>
      <span style="font-size:12px;font-weight:600;font-family:var(--mo)">${$m(t.total)}</span>
    </div>
    ${abierto?`<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--br)">${itemsTicketHtml(t.items)}</div>`:`<div style="font-size:9px;color:var(--tx3);font-family:var(--mo);margin-top:3px">Tocá para ver qué mercadería incluye</div>`}
  </div>`;
  }).join(''):`<div style="font-size:11px;color:var(--tx3);font-family:var(--mo);padding:6px 0">No hay facturas pendientes de este cliente</div>`;
  // Precios especiales: una fila por variante, vacío = precio de lista
  const precioRows=sgV().map(g=>{const vars=S.vr.filter(v=>v.group_id===g.id);if(!vars.length)return'';
    return`<tr><td colspan="3" style="padding:5px 8px;font-size:9px;color:var(--tx3);text-transform:uppercase;letter-spacing:.5px;border-top:1px solid var(--br)">${esc(g.name)}</td></tr>`
      +vars.map(v=>{const esp=S.cp.find(p=>p.cliente_id===id&&p.variant_id===v.id);return`<tr><td style="padding-left:14px;font-size:11px">${esc(v.name)}</td><td style="font-family:var(--mo);font-size:10px;color:var(--tx3)">${$m(v.price)}</td><td><input type="number" class="ip" placeholder="lista" value="${esp?esp.price:''}" onchange="setPrecioCliente('${id}','${v.id}',this.value)" style="${esp?'color:var(--ac);font-weight:600':''}"></td></tr>`;}).join('');
  }).join('');
  const nEsp=S.cp.filter(p=>p.cliente_id===id).length;
  return`<button class="btn" onclick="volverCCList()" style="margin-bottom:10px;padding:6px 12px;font-size:11px">← Volver a clientes</button>
  <div class="blk">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
      <div style="flex:1"><input type="text" class="ip" value="${esc(c.nombre)}" onchange="updCliente('${c.id}','nombre',this.value)" style="width:100%;font-size:15px;font-weight:700"><input type="text" class="ip" value="${esc(c.telefono||'')}" placeholder="Teléfono / nota" onchange="updCliente('${c.id}','telefono',this.value)" style="width:100%;margin-top:4px;font-size:11px"></div>
      ${sesion?.rol==='dueno'?`<button class="dbtn" onclick="delClienteCC('${c.id}')">✕</button>`:''}
    </div>
    <div class="kpis t3" style="margin-top:10px">
      <div class="kc"><div class="kl">Saldo Cta.Cte.</div><div class="kv" style="font-size:15px;color:${saldo>0?'var(--rd)':saldo<0?'var(--gn)':'var(--tx2)'}">${$m(saldo)}</div><div class="kh">${saldo>0?'nos debe':saldo<0?'a favor':'al día'}</div></div>
      <div class="kc"><div class="kl">Últimos 30 días</div><div class="kv" style="font-size:15px">${fQ(r.kg30,'kg')}</div><div class="kh">${$m(r.monto30)}</div></div>
      <div class="kc"><div class="kl">Última compra</div><div class="kv" style="font-size:15px;color:${r.dias!=null&&r.dias>7?'var(--rd)':'var(--tx)'}">${r.dias==null?'—':r.dias===0?'hoy':r.dias+' d'}</div><div class="kh">${r.ult?fDL(r.ult):''}</div></div>
    </div>
  </div>
  <div class="blk"><div class="bt" onclick="toggleDetail('cc-precios')" style="cursor:pointer">★ Precios especiales ${nEsp?`(${nEsp})`:''} ▾</div>
    <div id="cc-precios" style="display:${nEsp?'block':'none'}">
      <div style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin-bottom:6px">Cargá el precio que le cobrás a este cliente. Vacío = precio de lista. En Caja, al elegir el cliente, el ticket usa estos precios solo.</div>
      <table style="width:100%"><thead><tr><th>Variante</th><th>Lista</th><th>Precio cliente</th></tr></thead><tbody>${precioRows||'<tr><td colspan="3" class="empty-row">Sin variantes</td></tr>'}</tbody></table>
    </div>
  </div>
  ${pendientes.length||Math.abs(saldo)>=1?`<div class="blk"><div class="bt">Registrar pago de Cta.Cte.</div>
    <div style="font-size:10px;color:var(--tx3);font-family:var(--mo);margin-bottom:8px">Tildá qué facturas está pagando el cliente</div>
    ${pendRows}
    <div style="display:flex;justify-content:space-between;padding:6px 0;font-size:12px;font-weight:700;border-top:1px solid var(--br);margin-top:4px"><span>Total seleccionado</span><span id="cc-total-sel" style="font-family:var(--mo)">${$m(pendientes.filter(t=>ccSeleccionados.has(t.ticketId)).reduce((s,t)=>s+t.total,0))}</span></div>
    <div class="fr" style="margin-top:8px">
      <div class="fl"><label>Método</label><select id="cc-pago-metodo" onchange="toggleCCPagoMixto()"><option value="efectivo">Efectivo</option><option value="transferencia">Transferencia</option><option value="mixto">Mixto</option></select></div>
    </div>
    <div id="cc-pago-mixto-fields" style="display:none">
      <div class="fr">
        <div class="fl"><label>Transferencia $</label><input type="number" id="cc-pago-tr" placeholder="0"></div>
        <div class="fl" style="font-size:10px;color:var(--tx3);font-family:var(--mo);align-self:flex-end;padding-bottom:6px">El resto se toma como efectivo</div>
      </div>
    </div>
    <button class="btn btnp" onclick="registrarPagoCC('${id}')" style="width:100%;margin-top:6px">✓ Registrar pago</button>
  </div>`:''}
  <div class="sh">Historial de compras y pagos</div>${movRows}`;
}
function toggleCCSeleccion(tid,checked){
  if(checked)ccSeleccionados.add(tid);else ccSeleccionados.delete(tid);
  calcSeleccionCC();
}
function calcSeleccionCC(){
  let tot=0;
  document.querySelectorAll('.cc-tk-check').forEach(c=>{if(c.checked)tot+=parseFloat(c.dataset.monto)||0;});
  const el=document.getElementById('cc-total-sel');if(el)el.textContent=$m(tot);
}
function toggleCCPagoMixto(){
  const sel=document.getElementById('cc-pago-metodo'),f=document.getElementById('cc-pago-mixto-fields');
  if(f)f.style.display=sel?.value==='mixto'?'block':'none';
}
function ventaRow(v){return{id:v.id,day:v.day,ticket_id:v.ticket_id,variant_id:v.variant_id,group_id:v.group_id,qty:v.qty,stock_used:v.stock_used,price_unit:v.price_unit,descuento_pct:v.descuento_pct,total:v.total,costo_unit_venta:v.costo_unit_venta,pago:v.pago,pago_ef:v.pago_ef,pago_tr:v.pago_tr,cliente_id:v.cliente_id||v.cliente_cc_id||null,cliente_cc_id:v.cliente_cc_id,cc_pagado:v.cc_pagado,usuario:v.usuario,time:v.time};}
function registrarPagoCC(clienteId){
  const c=S.ccl.find(x=>x.id===clienteId);if(!c)return;
  const pendientes=ticketsPendientesCC(clienteId);
  const seleccionados=pendientes.filter(t=>ccSeleccionados.has(t.ticketId));
  if(!seleccionados.length)return alert('Tildá al menos una factura pendiente para registrar el pago');
  const totalSel=seleccionados.reduce((s,t)=>s+t.total,0);
  const metodo=document.getElementById('cc-pago-metodo')?.value;
  let ef=0,tr=0;
  if(metodo==='mixto'){
    tr=parseFloat(document.getElementById('cc-pago-tr')?.value)||0;
    if(tr<=0||tr>=totalSel)return alert(`En pago mixto cargá cuánto entró por transferencia (menos de ${$m(totalSel)}). El resto se toma como efectivo.`);
    ef=totalSel-tr;
  }else if(metodo==='efectivo'){ef=totalSel;}
  else{tr=totalSel;}
  const fechas=[...new Set(seleccionados.map(t=>fDL(t.day)))].join(', ');
  const desc=`Pago Cta.Cte: ${c.nombre} — Ticket${seleccionados.length>1?'s':''} del ${fechas}`;
  const rows=[];
  if(ef>0)rows.push({id:uid(),day,tipo:'ingreso',descripcion:desc,metodo:'efectivo',monto:ef,cliente_cc_id:clienteId,usuario:sesion?.nombre||'—',time:arTime()});
  if(tr>0)rows.push({id:uid(),day,tipo:'ingreso',descripcion:desc,metodo:'transferencia',monto:tr,cliente_cc_id:clienteId,usuario:sesion?.nombre||'—',time:arTime()});
  if(!S.caja[day])S.caja[day]=[];S.caja[day].push(...rows);
  // Marcar como pagadas las ventas de los tickets seleccionados
  const ticketIds=new Set(seleccionados.map(t=>t.ticketId));
  const ventasActualizadas=[];
  Object.values(S.ve).forEach(arr=>arr.forEach(v=>{
    if(v.cliente_cc_id===clienteId&&ticketIds.has(v.ticket_id||v.id)){v.cc_pagado=true;ventasActualizadas.push(v);}
  }));
  ccSeleccionados=new Set();
  save();render();toast(`Pago de ${c.nombre} registrado ✓`);
  rows.forEach(r=>sbUp('caja_movimientos',r));
  if(ventasActualizadas.length)sbUp('ventas',ventasActualizadas.map(ventaRow));
}

function rComprasMerc(){
  const todC=S.co.filter(c=>c.day===day);
  const sgVOpts=sgV().map(g=>`<option value="sgv_${g.id}" data-u="${g.unit||'kg'}">${esc(g.name)} (${g.unit||'kg'})</option>`).join('');
  const insOpts=S.ins.map(i=>`<option value="ins_${i.id}" data-u="${i.unit}">${esc(i.name)} (${i.unit})</option>`).join('');
  const allOpts=`<optgroup label="Stock de venta">${sgVOpts||'<option disabled>Sin grupos</option>'}</optgroup><optgroup label="Insumos">${insOpts||'<option disabled>Sin insumos</option>'}</optgroup>`;
  const cards=todC.length?todC.map(c=>{const items=S.coi.filter(i=>i.compra_id===c.id);const pi=[];if(c.pago_efectivo>0)pi.push(`Ef: ${$m(c.pago_efectivo)}`);if(c.pago_transferencia>0)pi.push(`Tr: ${$m(c.pago_transferencia)}`);
    return`<div class="lote-card"><div class="lote-card-header"><div><div style="font-size:13px;font-weight:600">${esc(c.proveedor)}</div><div style="font-size:10px;color:var(--tx3);font-family:var(--mo)">${c.time||''}${c.nro_factura?' · F/'+esc(c.nro_factura):''}</div>${pi.length?`<div style="font-size:10px;color:var(--tx2);font-family:var(--mo)">${pi.join(' + ')}</div>`:''}</div><div style="text-align:right"><div style="font-size:13px;font-family:var(--mo);color:var(--ac)">${$m(c.total)}</div><button class="dbtn" onclick="delCompra('${c.id}')" style="margin-top:3px">✕</button></div></div>
    ${items.length?`<div class="lote-card-items">${items.map(i=>`<div style="font-size:10px;color:var(--tx2);padding:2px 0">${esc(i.descripcion)}: ${i.qty_compra} ${i.unit_compra||''} ${i.qty_real?'→ '+fQ(i.qty_real,i.unit_real):''} — ${$m(i.precio_total)}${i.cost_unit_calculado?' ('+$d2(i.cost_unit_calculado)+'/'+i.unit_real+')':''}</div>`).join('')}</div>`:''}
    </div>`;
  }).join(''):`<div class="empty-row">Sin compras hoy</div>`;
  return`<div class="info-box">🛒 Al guardar: actualiza stock y costos, y registra el total como gasto del día.</div>
  <div class="sh">Nueva factura</div>
  <div class="blk"><div class="bt">Datos de la factura</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Proveedor</label><input type="text" id="cp-prov" placeholder="Ej: Avícola Don Juan"></div><div class="fl" style="max-width:110px"><label>Nro. Factura</label><input type="text" id="cp-fact" placeholder="opcional"></div></div>
    <div class="fr"><div class="fl"><label>Nota</label><input type="text" id="cp-note" placeholder=""></div></div>
    <div style="font-size:9px;color:var(--tx2);font-family:var(--mo);margin-bottom:6px;margin-top:4px">FORMA DE PAGO</div>
    <div class="fr"><div class="fl"><label>Efectivo $</label><input type="number" id="cp-ef" placeholder="0" oninput="calcCpTotal()"></div><div class="fl"><label>Transferencia $</label><input type="number" id="cp-tr" placeholder="0" oninput="calcCpTotal()"></div><div class="fl"><label>Total</label><input type="text" id="cp-tot" readonly style="color:var(--ac)"></div></div>
  </div>
  <div class="blk"><div class="bt">Artículos</div>
    <div id="cp-items-list"></div>
    <div class="sep"></div>
    <div style="font-size:9px;color:var(--tx2);font-family:var(--mo);margin-bottom:7px">AGREGAR ARTÍCULO</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Descripción</label><input type="text" id="ci-desc" placeholder="Ej: Cajón pollo, Pan rallado..."></div><div class="fl" style="max-width:110px"><label>Actualiza</label><select id="ci-tipo" onchange="onCiTipo()"><option value="sgv">Stock de venta</option><option value="ins">Insumo</option><option value="mpc">Materia prima cruda (cajón)</option><option value="otro">Ninguno</option></select></div></div>
    <div class="fr" id="ci-item-row"><div class="fl"><label>Artículo</label><select id="ci-ref">${sgVOpts||'<option disabled>Sin grupos</option>'}</select></div></div>
    <div style="display:flex;gap:10px;margin-top:4px;flex-wrap:wrap">
      <label style="display:flex;align-items:center;gap:5px;font-size:11px;cursor:pointer;font-family:var(--mo)"><input type="checkbox" id="ci-upd-stock" checked style="width:auto;accent-color:var(--ac)"> Actualizar stock</label>
      <label style="display:flex;align-items:center;gap:5px;font-size:11px;cursor:pointer;font-family:var(--mo)"><input type="checkbox" id="ci-upd-cost" checked style="width:auto;accent-color:var(--ac)"> Actualizar costo/u</label>
    </div>
    <div class="fr" style="margin-top:6px"><div class="fl" style="max-width:70px"><label>Cant. compra</label><input type="number" id="ci-qtyc" placeholder="4" min="0.001" step="0.001"></div><div class="fl" style="max-width:90px"><label>Unidad compra</label><input type="text" id="ci-uc" placeholder="cajón, bolsa..."></div><div class="fl" style="max-width:70px"><label>Cant. real</label><input type="number" id="ci-qtyr" placeholder="74" min="0.001" step="0.001"></div><div class="fl" style="max-width:80px"><label>Precio total $</label><input type="number" id="ci-precio" placeholder="0" oninput="calcCostoUnitario()"></div><button class="btn" onclick="addCompraItem()" style="align-self:flex-end;padding:6px 10px;font-size:11px">+</button></div>
    <div style="font-size:9px;color:var(--tx3);font-family:var(--mo);margin-top:3px" id="costo-preview"></div>
    <div style="font-size:9px;color:var(--tx3);font-family:var(--mo)">Cant. real = kg reales. Ej: 4 cajones → 74kg → costo/kg = total ÷ 74</div>
  </div>
  <button class="btn btnp" onclick="saveCompra()" style="width:100%;margin-top:4px">✓ Guardar factura</button>
  <div class="sh">Compras de hoy</div>${cards}`;
}
function calcCpTotal(){const ef=parseFloat(document.getElementById('cp-ef')?.value)||0,tr=parseFloat(document.getElementById('cp-tr')?.value)||0;const tot=document.getElementById('cp-tot');if(tot)tot.value=ef+tr>0?$m(ef+tr):'';}
function onCiTipo(){
  const tipo=document.getElementById('ci-tipo')?.value;
  const refEl=document.getElementById('ci-ref');
  const row=document.getElementById('ci-item-row');
  if(!refEl)return;
  if(tipo==='sgv'){
    if(row)row.style.display='';
    refEl.innerHTML=sgV().map(g=>`<option value="sgv_${g.id}" data-u="${g.unit||'kg'}">${esc(g.name)} (${g.unit||'kg'})</option>`).join('')||'<option disabled>Sin grupos</option>';
  }else if(tipo==='ins'){
    if(row)row.style.display='';
    refEl.innerHTML=S.ins.map(i=>`<option value="ins_${i.id}" data-u="${i.unit}">${esc(i.name)} (${i.unit})</option>`).join('')||'<option disabled>Sin insumos</option>';
  }else{
    if(row)row.style.display='none';
  }
  calcCostoUnitario();
}
function ciCurrentUnit(){const tipo=document.getElementById('ci-tipo')?.value;if(tipo==='mpc')return'kg';const refEl=document.getElementById('ci-ref');return refEl?.options[refEl.selectedIndex]?.dataset?.u||'kg';}
function calcCostoUnitario(){const precio=parseFloat(document.getElementById('ci-precio')?.value)||0,qtyR=parseFloat(document.getElementById('ci-qtyr')?.value)||0,qtyC=parseFloat(document.getElementById('ci-qtyc')?.value)||0,prev=document.getElementById('costo-preview');if(!prev)return;if(precio&&(qtyR||qtyC)){const base=qtyR||qtyC,costo=precio/base,unit=ciCurrentUnit();prev.textContent=`→ Costo calculado: ${$d2(costo)}/${unit}`;prev.style.color='var(--ac)';}else{prev.textContent='';}}
function renderCompraItems(){
  const list=document.getElementById('cp-items-list');if(!list)return;
  if(!compraItems.length){list.innerHTML='';return;}
  const tot=compraItems.reduce((s,x)=>s+x.precio_total,0);
  list.innerHTML=compraItems.map((x,i)=>`<div class="compra-item"><div style="display:flex;justify-content:space-between;align-items:flex-start"><div><div style="font-size:12px;font-weight:500">${esc(x.descripcion)}</div><div style="font-size:10px;color:var(--tx3);font-family:var(--mo)">${x.qty_compra} ${x.unit_compra||''} ${x.qty_real?'→ '+fQ(x.qty_real,x.unit_real):''}</div><div style="font-size:10px;color:var(--ac);font-family:var(--mo)">${$m(x.precio_total)} · costo: ${$d2(x.cost_unit_calculado||0)}/${x.unit_real||'kg'}</div><div style="font-size:9px;color:var(--tx3);font-family:var(--mo)">${x.upd_stock?'✓ stock':'○'} · ${x.upd_cost?'✓ costo':'○'}</div></div><button class="dbtn" onclick="rmCompraItem(${i})">✕</button></div></div>`).join('')
    +`<div style="text-align:right;font-size:12px;font-family:var(--mo);color:var(--ac);padding:5px 0">Total artículos: ${$m(tot)}</div>`;
}
function addCompraItem(){
  const desc=document.getElementById('ci-desc')?.value.trim();
  const tipoEl=document.getElementById('ci-tipo'),refEl=document.getElementById('ci-ref'),refOpt=refEl?.options[refEl.selectedIndex],refVal=refEl?.value;
  const tipo=tipoEl?.value||'sgv';
  const qtyC=parseFloat(document.getElementById('ci-qtyc')?.value)||0,uc=document.getElementById('ci-uc')?.value.trim()||'unidad',qtyR=parseFloat(document.getElementById('ci-qtyr')?.value)||0,precio=parseFloat(document.getElementById('ci-precio')?.value)||0;
  const updStock=document.getElementById('ci-upd-stock')?.checked!==false,updCost=document.getElementById('ci-upd-cost')?.checked!==false;
  if(!desc||!qtyC||!precio)return alert('Completá descripción, cantidad y precio');
  if(tipo==='mpc'&&!qtyR)return alert('Para materia prima cruda, cargá los kg reales (cant. real)');
  const unitReal=tipo==='mpc'?'kg':(refOpt?.dataset?.u||'kg'),base=qtyR||qtyC,costCalc=precio/base;
  const isStock=tipo==='sgv',isIns=tipo==='ins',isMpc=tipo==='mpc',ref_id=(isStock||isIns)?(refVal?.replace(/^(sgv_|ins_)/,'')||null):null;
  compraItems.push({descripcion:desc,tipo_destino:isStock?'stock_venta':isIns?'insumo':isMpc?'materia_prima_cruda':'otro',ref_id,qty_compra:qtyC,unit_compra:uc,qty_real:qtyR||qtyC,unit_real:unitReal,precio_total:precio,cost_unit_calculado:costCalc,upd_stock:isMpc?false:updStock,upd_cost:isMpc?false:updCost,usado:false});
  document.getElementById('ci-desc').value='';document.getElementById('ci-qtyc').value='';document.getElementById('ci-qtyr').value='';document.getElementById('ci-precio').value='';
  const prev=document.getElementById('costo-preview');if(prev)prev.textContent='';renderCompraItems();
}
function rmCompraItem(i){compraItems.splice(i,1);renderCompraItems();}
function saveCompra(){
  const prov=document.getElementById('cp-prov')?.value.trim(),fact=document.getElementById('cp-fact')?.value.trim(),note=document.getElementById('cp-note')?.value.trim();
  const ef=parseFloat(document.getElementById('cp-ef')?.value)||0,tr=parseFloat(document.getElementById('cp-tr')?.value)||0;
  if(!prov)return alert('Ingresá el proveedor');if(!compraItems.length)return alert('Agregá al menos un artículo');
  const total=(ef+tr)||compraItems.reduce((s,x)=>s+x.precio_total,0);
  const cId=uid(),gastoId=uid();
  const compra={id:cId,day,proveedor:prov,nro_factura:fact||null,total,pago_efectivo:ef,pago_transferencia:tr,gasto_id:gastoId,note:note||null,usuario:sesion?.nombre||'—',time:arTime()};
  const items=compraItems.map(x=>({id:uid(),compra_id:cId,...x}));
  S.co.push(compra);S.coi.push(...items);
  items.forEach(x=>{
    const qty=x.qty_real||x.qty_compra;
    if(x.tipo_destino==='stock_venta'){
      const g=S.sg.find(sg=>sg.id===x.ref_id);if(!g)return;
      if(x.upd_stock!==false)g.stock_qty=(g.stock_qty||0)+qty;
      if(x.upd_cost!==false)recalcCosto(g); // el costo sale del promedio de lotes (esta compra ya está en la lista)
    }
    else if(x.tipo_destino==='insumo'){
      const ins=S.ins.find(i=>i.id===x.ref_id);if(!ins)return;
      if(x.upd_stock!==false)ins.stock_qty=(ins.stock_qty||0)+qty;
      if(x.upd_cost!==false){ins.costUnit=x.cost_unit_calculado||ins.costUnit;ins.cost_unit=ins.costUnit;}
    }
    // materia_prima_cruda: no toca stock, queda pendiente de trozar en Producción → Corte
  });
  const gastoRow={id:gastoId,day,descripcion:'Compra: '+prov+(fact?' F/'+fact:''),cat:'Materia prima',amount:total,metodo:tr>ef?'transferencia':'efectivo',pago_efectivo:ef,pago_transferencia:tr,usuario:sesion?.nombre||'—',time:arTime()};
  if(!S.ga[day])S.ga[day]=[];S.ga[day].push(gastoRow);
  compraItems=[];save();render();
  // En orden: la compra, sus ítems y su gasto. Si se corta internet, la cola los sube después en este mismo orden.
  sbUp('compras',compra);
  if(items.length)sbUp('compras_items',items);
  sbUp('gastos',gastoRow);
  subirGrupos(items.filter(x=>x.tipo_destino==='stock_venta').map(x=>x.ref_id));
  subirInsumos(items.filter(x=>x.tipo_destino==='insumo').map(x=>x.ref_id));
}
function delCompra(id){
  if(sesion?.rol!=='dueno')return alert('Solo el Dueño puede eliminar. Pedile que ingrese con su PIN para borrar esto.');
  const c=S.co.find(x=>x.id===id);if(!c)return;
  const items=S.coi.filter(x=>x.compra_id===id);
  if(items.some(x=>x.tipo_destino==='materia_prima_cruda'&&x.usado))return alert('No se puede eliminar: esta compra ya fue usada en un Corte. Eliminá primero el corte correspondiente.');
  if(!confirm('¿Eliminar factura? Se revierte el stock y se recalcula el costo sin esta compra.'))return;
  items.forEach(x=>{
    if(x.tipo_destino==='stock_venta'&&x.upd_stock!==false){const g=S.sg.find(sg=>sg.id===x.ref_id);if(g)g.stock_qty=(g.stock_qty||0)-(x.qty_real||x.qty_compra);}
    else if(x.tipo_destino==='insumo'&&x.upd_stock!==false){const ins=S.ins.find(i=>i.id===x.ref_id);if(ins)ins.stock_qty=(ins.stock_qty||0)-(x.qty_real||x.qty_compra);}
  });
  if(c.gasto_id){Object.keys(S.ga).forEach(d=>{S.ga[d]=(S.ga[d]||[]).filter(g=>g.id!==c.gasto_id);});}
  S.co=S.co.filter(x=>x.id!==id);S.coi=S.coi.filter(x=>x.compra_id!==id);
  items.filter(x=>x.tipo_destino==='stock_venta').forEach(x=>{const g=S.sg.find(sg=>sg.id===x.ref_id);if(g)recalcCosto(g);});
  save();render();
  if(c.gasto_id)sbDel('gastos',c.gasto_id);
  sbDel('compras',id); // los ítems se borran solos en la base (cascade)
  subirGrupos(items.filter(x=>x.tipo_destino==='stock_venta').map(x=>x.ref_id));
  subirInsumos(items.filter(x=>x.tipo_destino==='insumo').map(x=>x.ref_id));
}

/* ══ GASTOS DEL LOCAL ════════════════════════════════════════════
   Alquiler, luz, arreglos, impuestos... Cuentan en el resultado del mes,
   pero NO en el cierre de caja (no salen del cajón del día).           */
function rGastosLocal(){
  const ym=day.slice(0,7);
  const gs=Object.entries(S.ga).filter(([d])=>d.startsWith(ym)).flatMap(([,g])=>g).filter(g=>g.fuera_caja).sort((a,b)=>a.day.localeCompare(b.day));
  const tot=gs.reduce((s,g)=>s+g.amount,0);
  const cats={};gs.forEach(g=>{cats[g.cat]=(cats[g.cat]||0)+g.amount});
  const cH=Object.entries(cats).sort((a,b)=>b[1]-a[1]).map(([c,v])=>`<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--br)"><span style="font-size:11px;color:var(--tx2)">${esc(c)}</span><span style="font-family:var(--mo);font-size:12px">${$m(v)}</span></div>`).join('');
  const rows=gs.length?gs.map(g=>`<tr><td>${fD(g.day)}</td><td style="max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(g.descripcion)}</td><td><span class="tag tg">${esc((g.cat||'').slice(0,8))}</span></td><td style="font-family:var(--mo)">${$m(g.amount)}</td><td>${sesion?.rol==='dueno'?`<button class="dbtn" onclick="delGa('${g.id}')">✕</button>`:''}</td></tr>`).join(''):`<tr><td colspan="5" class="empty-row">Sin gastos del local este mes</td></tr>`;
  return`<div class="info-box">🏠 Gastos del local: alquiler, luz, arreglos, impuestos… Cuentan en el <b>resultado del mes</b>, pero <b>no</b> en el cierre de caja, porque no salen del cajón.</div>
  <div class="kpis"><div class="kc hi"><div class="kl">Gastos del local — ${fM(ym)}</div><div class="kv r">${$m(tot)}</div><div class="kh">${gs.length} registro${gs.length===1?'':'s'}</div></div></div>
  <div class="blk"><div class="bt">Cargar gasto del local — ${fDL(day)}</div>
    <div class="fr"><div class="fl" style="flex:2"><label>Descripción</label><input type="text" id="gl-d" placeholder="Ej: Alquiler octubre, factura de luz..."></div>
      <div class="fl"><label>Categoría</label><select id="gl-c">${CATS_LOCAL.map(c=>`<option>${esc(c)}</option>`).join('')}</select></div></div>
    <div class="fr"><div class="fl"><label>Monto $</label><input type="number" id="gl-a" placeholder="0"></div>
      <div class="fl" style="max-width:120px"><label>Pagado con</label><select id="gl-met"><option value="transferencia">Transferencia</option><option value="efectivo">Efectivo (no del cajón)</option></select></div>
      <button class="btn btnr" onclick="addGastoLocal()" style="align-self:flex-end">+ Gasto</button></div>
    <div style="font-size:9px;color:var(--tx3);font-family:var(--mo);margin-top:3px">Se carga en la fecha que tenés arriba. Para otra fecha, cambiala con las flechas.</div>
  </div>
  ${cH?`<div class="blk"><div class="bt">Por categoría</div>${cH}</div>`:''}
  <div class="tbk"><div class="tt">Gastos del local — ${fM(ym)}</div>
    <table><thead><tr><th>Fecha</th><th>Descripción</th><th>Cat.</th><th>Monto</th><th></th></tr></thead><tbody>${rows}</tbody></table>
  </div>`;
}
function addGastoLocal(){
  const d2=document.getElementById('gl-d')?.value.trim(),c=document.getElementById('gl-c')?.value,a=parseFloat(document.getElementById('gl-a')?.value)||0,met=document.getElementById('gl-met')?.value||'transferencia';
  if(!d2||!a)return alert('Completá descripción y monto');
  const row={id:uid(),day,descripcion:d2,cat:c,amount:a,metodo:met,fuera_caja:true,usuario:sesion?.nombre||'—',time:arTime()};
  if(!S.ga[day])S.ga[day]=[];S.ga[day].push(row);save();render();toast('Gasto del local guardado ✓');
  sbUp('gastos',row);
}
function setComprasTab(t){comprasTab=t;render();}
function rCompras(){
  return`<div class="prod-tabs">
    <button class="prod-tab ${comprasTab==='mercaderia'?'active':''}" onclick="setComprasTab('mercaderia')">🛒 Mercadería</button>
    <button class="prod-tab ${comprasTab==='local'?'active':''}" onclick="setComprasTab('local')">🏠 Gastos del local</button>
  </div>
  ${comprasTab==='local'?rGastosLocal():rComprasMerc()}`;
}

/* ══ REPORTES ══════════════════════════════════════════════════════ */
function getMths(){const s=new Set();Object.keys(S.ve).forEach(d=>s.add(d.slice(0,7)));Object.keys(S.ga).forEach(d=>s.add(d.slice(0,7)));return[...s].sort((a,b)=>b.localeCompare(a))}

// Calcula efectivo y transferencia correctamente deduplicando tickets mixtos
function calcEfTr(vs){
  let ef=0,tr=0;
  const seen=new Set();
  // Primero sumar ventas puras
  vs.forEach(v=>{
    if(v.pago==='Efectivo'){ef+=v.total;}
    else if(v.pago==='Transferencia'){tr+=v.total;}
    else if(v.pago==='mixto'){
      const tk=v.ticket_id||v.id;
      if(!seen.has(tk)){seen.add(tk);ef+=(v.pago_ef||0);tr+=(v.pago_tr||0);}
    }
  });
  return{ef,tr};
}

// Agrupa ventas por producto, repartiendo bien efectivo/transferencia incluso en tickets con pago mixto
// La Cta.Cte. suma solo los kg el día de la venta; la plata entra recién cuando se cobra (como ingreso)
function calcByG(vs){
  const ticketTot={};
  vs.forEach(v=>{const tk=v.ticket_id||v.id;ticketTot[tk]=(ticketTot[tk]||0)+v.total;});
  const byG={};
  vs.forEach(v=>{
    const g=S.sg.find(x=>x.id===v.group_id),gn=g?g.name:'Otros';
    if(!byG[gn])byG[gn]={qty:0,qtyCC:0,tot:0,ef:0,tr:0,costo:0,unit:g?.unit||''};
    byG[gn].qty+=(v.stock_used||0);
    if(v.pago==='cuenta_corriente'){byG[gn].qtyCC+=(v.stock_used||0);return;}
    byG[gn].tot+=v.total;
    byG[gn].costo+=(v.stock_used||0)*(v.costo_unit_venta||0);
    if(v.pago==='Efectivo')byG[gn].ef+=v.total;
    else if(v.pago==='Transferencia')byG[gn].tr+=v.total;
    else if(v.pago==='mixto'){
      const tk=v.ticket_id||v.id,tkTot=ticketTot[tk]||v.total;
      // proporción del ticket que fue efectivo/transferencia, aplicada a este ítem
      const ratioEf=tkTot>0?(v.pago_ef||0)/tkTot:0,ratioTr=tkTot>0?(v.pago_tr||0)/tkTot:0;
      byG[gn].ef+=v.total*ratioEf;byG[gn].tr+=v.total*ratioTr;
    }
  });
  return byG;
}
// Motor único para día/mes — evita tener la misma lógica escrita dos veces
function periodData(matchDay){
  const vs=Object.entries(S.ve).filter(([d])=>matchDay(d)).flatMap(([,v])=>v);
  // Ventas a Cuenta Corriente NO cuentan como "venta" todavía en ningún lado — recién suman cuando se cobran (más abajo, como ingreso)
  const vsCash=vs.filter(v=>v.pago!=='cuenta_corriente');
  const tv=vsCash.reduce((s,v)=>s+v.total,0);
  const{ef:tvEf,tr:tvTr}=calcEfTr(vsCash);
  const byG=calcByG(vs); // "Ventas por grupo": kg de todo lo vendido (incluye Cta.Cte.); $, costo y margen solo de lo cobrado en el momento
  const costoVentasTotal=Object.values(byG).reduce((s,g)=>s+g.costo,0);
  const margenBruto=tv-costoVentasTotal;
  const _compraIds=idsGastoCompra();
  const gasP=Object.entries(S.ga).filter(([d])=>matchDay(d)).flatMap(([,g])=>g).filter(g=>!_compraIds.has(g.id));
  const tg=gasP.reduce((s,g)=>s+g.amount,0); // gastos operativos: de caja + del local
  const tgLocal=gasP.filter(g=>g.fuera_caja).reduce((s,g)=>s+g.amount,0);
  const tCompras=S.co.filter(c=>matchDay(c.day)).reduce((s,c)=>s+c.total,0);
  const movs=Object.entries(S.caja).filter(([d])=>matchDay(d)).flatMap(([,m])=>m);
  const ingEf=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
  const ingTr=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='transferencia').reduce((s,m)=>s+m.monto,0);
  const egEf=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
  const egTr=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='transferencia').reduce((s,m)=>s+m.monto,0);
  const ingTotal=ingEf+ingTr,egTotal=egEf+egTr,resultado=tv+ingTotal-egTotal-tg-tCompras;
  const ingExtra=movs.filter(m=>m.tipo==='ingreso'&&!m.cliente_cc_id).reduce((s,m)=>s+m.monto,0); // ingresos manuales de verdad, sin contar cobros de Cta.Cte (esos ya son parte de la venta)
  const cobrosCC=movs.filter(m=>m.tipo==='ingreso'&&m.cliente_cc_id).reduce((s,m)=>s+m.monto,0);
  return{vs,tv,tvEf,tvTr,tg,tgLocal,tCompras,byG,ingEf,ingTr,egEf,egTr,ingTotal,egTotal,ingExtra,cobrosCC,resultado,costoVentasTotal,margenBruto};
}
function mData(ym){return periodData(d=>d.startsWith(ym));}
function dayData(d0){return periodData(d=>d===d0);}
function yrData(yr){const ms=[];for(let m=1;m<=12;m++){const ym=yr+'-'+m.toString().padStart(2,'0');const{tv,tg,tCompras,resultado}=mData(ym);ms.push({lbl:fM(ym).split(' ')[0],tv,tg:tg+tCompras,res:resultado})}return ms}

function rReportes(){
  const mths=getMths();if(!mths.includes(rMonth)&&mths.length)rMonth=mths[0];
  const yr=rMonth.split('-')[0];
  const mthTabs=mths.length?mths.map(m=>`<button class="mtab ${m===rMonth?'active':''}" onclick="setRM('${m}')">${fM(m)}</button>`).join(''):`<span style="font-size:10px;color:var(--tx3);font-family:var(--mo)">Sin datos aún</span>`;
  const rTabs=`<div class="mtabs" style="margin-bottom:8px">
    <button class="mtab ${rTab==='dia'?'active':''}" onclick="setRTab('dia')">📅 Día</button>
    <button class="mtab ${rTab==='mes'?'active':''}" onclick="setRTab('mes')">📆 Mes</button>
    <button class="mtab ${rTab==='anual'?'active':''}" onclick="setRTab('anual')">📈 Anual</button>
    <button class="mtab ${rTab==='financiero'?'active':''}" onclick="setRTab('financiero')">💹 Financiero</button>
    <button class="mtab ${rTab==='comparar'?'active':''}" onclick="setRTab('comparar')">🆚 Comparar</button>
  </div>`;
  if(rTab==='dia')return rTabs+rRepDia();
  if(rTab==='mes')return rTabs+rRepMes(mthTabs);
  if(rTab==='anual')return rTabs+rRepAnual(yr);
  if(rTab==='financiero')return rTabs+rRepFin(mthTabs);
  if(rTab==='comparar')return rTabs+rRepComparar(mths);
  return'';
}
// KPIs financieros de un mes — función única, reutilizada por Financiero y Comparar (evita tener la misma cuenta escrita en dos lugares)
function finKpis(ym){
  const{tv,tvEf,tvTr,tg,tgLocal,tCompras,ingEf,ingTr,egEf,egTr,ingTotal,egTotal,ingExtra,resultado}=mData(ym);
  const tGastoTotal=tg+tCompras;
  const ingresoTotal=tv+ingTotal;
  const margen=ingresoTotal>0?Math.round((resultado/ingresoTotal)*100):0;
  return{tv,tg,tgLocal,tCompras,tGastoTotal,ingresoTotal,margen,resultado,ingExtra,egTotal,efectivoTotal:tvEf+ingEf-egEf,digitalTotal:tvTr+ingTr-egTr};
}
// Gastos operativos por categoría de un mes — función única, reutilizada por Financiero y Comparar
function catGastosMes(ym,tCompras){
  const _compraIds=new Set(S.co.map(c=>c.gasto_id).filter(Boolean));
  const catGas={};
  Object.entries(S.ga).filter(([d])=>d.startsWith(ym)).flatMap(([,g])=>g).filter(g=>!_compraIds.has(g.id)).forEach(g=>{catGas[g.cat]=(catGas[g.cat]||0)+g.amount});
  if(tCompras>0)catGas['Materia prima (compras)']=(catGas['Materia prima (compras)']||0)+tCompras;
  return catGas;
}
function setRM(m){rMonth=m;go('reportes')}
function setRTab(t){rTab=t;go('reportes')}
function toggleDetail(id){const el=document.getElementById(id);if(el)el.style.display=el.style.display==='none'?'block':'none';}

function rRepDia(){
  const{vs,tv,tvEf,tvTr,byG,tg,tCompras,ingTotal,egTotal,ingEf,ingTr,egEf,egTr,resultado}=dayData(day);
  const bgRows=Object.entries(byG).sort((a,b)=>b[1].tot-a[1].tot).map(([n,d])=>{const marg=d.tot-d.costo,margPct=d.tot>0?Math.round((marg/d.tot)*100):0;return`<tr><td>${esc(n)}</td><td style="font-family:var(--mo)">${fQ(d.qty,d.unit)}${d.qtyCC?`<div style="font-size:9px;color:var(--pu,#a78bfa)">${fQ(d.qtyCC,d.unit)} Cta.Cte.</div>`:''}</td><td style="font-family:var(--mo)">${$m(d.tot)}</td><td style="font-family:var(--mo);color:var(--gn)">${$m(d.ef)}</td><td style="font-family:var(--mo);color:var(--bl)">${$m(d.tr)}</td><td style="font-family:var(--mo);color:var(--tx2)">${$m(d.costo)}</td><td style="font-family:var(--mo);color:${marg>=0?'var(--gn)':'var(--rd)'}">${$m(marg)} (${margPct}%)</td></tr>`;}).join('')||`<tr><td colspan="7" class="empty-row">Sin ventas</td></tr>`;
  // tickets del dia agrupados
  const byTicket={};vs.forEach(v=>{const tk=v.ticket_id||v.id;if(!byTicket[tk])byTicket[tk]={items:[],total:0,pago:v.pago,time:v.time||''};byTicket[tk].items.push(v);byTicket[tk].total+=v.total;});
  const tktRows=Object.entries(byTicket).sort((a,b)=>timeToMin(a[1].time)-timeToMin(b[1].time)).map(([tid,tk])=>`<tr><td>${tk.time}</td><td>${tk.items.length} ítem(s)</td><td style="font-family:var(--mo)">${$m(tk.total)}</td><td><span class="tag tv">${tk.pago==='cuenta_corriente'?'CC':(tk.pago||'').slice(0,3)}</span></td></tr>`).join('')||`<tr><td colspan="4" class="empty-row">Sin tickets</td></tr>`;
  const _compraIds=new Set(S.co.map(c=>c.gasto_id).filter(Boolean));
  const gsRows=(S.ga[day]||[]).filter(g=>!_compraIds.has(g.id)).map(g=>`<tr><td>${g.time||''}</td><td>${esc(g.descripcion)}</td><td><span class="tag tg">${(g.cat||'').slice(0,5)}</span>${g.fuera_caja?' <span class="tag to">local</span>':''}</td><td>${$m(g.amount)}</td></tr>`).join('')||`<tr><td colspan="4" class="empty-row">Sin gastos operativos</td></tr>`;
  const compHoy=S.co.filter(c=>c.day===day).map(c=>`<tr><td>${c.time||''}</td><td>${esc(c.proveedor)}</td><td><span class="tag to">compra</span></td><td>${$m(c.total)}</td></tr>`).join('');
  return`
  <div style="font-size:11px;color:var(--tx2);font-family:var(--mo);margin-bottom:10px">📅 ${fDL(day)}</div>
  <div class="kpis t3">
    <div class="kc hi" onclick="toggleDetail('det-vd')" style="cursor:pointer"><div class="kl">Ventas ▾</div><div class="kv a">${$m(tv)}</div><div class="kh">${Object.keys(byTicket).length} tickets</div></div>
    <div class="kc" onclick="toggleDetail('det-gd')" style="cursor:pointer"><div class="kl">Gastos ▾</div><div class="kv r">${$m(tg+tCompras)}</div><div class="kh" style="color:var(--tx3)">op. ${$m(tg)} · comp. ${$m(tCompras)}</div></div>
    <div class="kc"><div class="kl">Resultado</div><div class="kv ${resultado>=0?'g':'r'}">${$m(resultado)}</div></div>
  </div>
  <div id="det-vd" style="display:none">
    <div class="tbk"><div class="tt">Tickets del día</div><table><thead><tr><th>Hora</th><th>Ítems</th><th>Total</th><th>Pago</th></tr></thead><tbody>${tktRows}</tbody></table></div>
    <div class="tbk"><div class="tt">Por grupo</div><div class="tbk-hint">→ deslizá para ver Costo y Margen</div><div class="tbk-scroll"><table><thead><tr><th>Grupo</th><th>Cantidad</th><th>Total</th><th>Efectivo</th><th>Transf.</th><th>Costo</th><th>Margen</th></tr></thead><tbody>${bgRows}</tbody></table></div></div>
  </div>
  <div id="det-gd" style="display:none">
    ${tg>0?`<div class="tbk"><div class="tt">Gastos operativos</div><table><thead><tr><th>Hora</th><th>Descripción</th><th>Cat.</th><th>Monto</th></tr></thead><tbody>${gsRows}</tbody></table></div>`:''}
    ${compHoy?`<div class="tbk"><div class="tt">Compras del día</div><table><thead><tr><th>Hora</th><th>Proveedor</th><th>Tipo</th><th>Total</th></tr></thead><tbody>${compHoy}</tbody></table></div>`:''}
    ${!tg&&!compHoy?`<div style="font-size:11px;color:var(--tx3);font-family:var(--mo);padding:8px 0">Sin gastos ni compras</div>`:''}
  </div>
  <div class="kpis">
    <div class="kc"><div class="kl">Efectivo</div><div class="kv g">${$m(tvEf+ingEf-egEf)}</div></div>
    <div class="kc"><div class="kl">Digital</div><div class="kv b">${$m(tvTr+ingTr-egTr+(S.cierres?.[day]?.fondo_digital_manual||0))}</div></div>
  </div>
  <button class="btn btng" onclick="exportExcel()" style="width:100%;margin-top:6px">⬇ Exportar todo a Excel</button>`;
}

function rRepMes(mthTabs){
  const{tv,tvEf,tvTr,tg,tCompras,byG,ingEf,ingTr,egEf,egTr,ingTotal,egTotal,resultado}=mData(rMonth);
  const bgRows=Object.entries(byG).sort((a,b)=>b[1].tot-a[1].tot).map(([n,d])=>{const marg=d.tot-d.costo,margPct=d.tot>0?Math.round((marg/d.tot)*100):0;return`<tr><td>${esc(n)}</td><td style="font-family:var(--mo)">${fQ(d.qty,d.unit)}${d.qtyCC?`<div style="font-size:9px;color:var(--pu,#a78bfa)">${fQ(d.qtyCC,d.unit)} Cta.Cte.</div>`:''}</td><td style="font-family:var(--mo)">${$m(d.tot)}</td><td style="font-family:var(--mo);color:var(--gn)">${$m(d.ef)}</td><td style="font-family:var(--mo);color:var(--bl)">${$m(d.tr)}</td><td style="font-family:var(--mo);color:var(--tx2)">${$m(d.costo)}</td><td style="font-family:var(--mo);color:${marg>=0?'var(--gn)':'var(--rd)'}">${$m(marg)} (${margPct}%)</td></tr>`;}).join('')||`<tr><td colspan="7" class="empty-row">Sin datos</td></tr>`;
  const gastosMes=Object.entries(S.ga).filter(([d])=>d.startsWith(rMonth)).flatMap(([d,gs])=>gs.map(g=>({...g,_d:d})));
  const _compraIdsMes=new Set(S.co.map(c=>c.gasto_id).filter(Boolean));
  const gsRows=gastosMes.filter(g=>!_compraIdsMes.has(g.id)).sort((a,b)=>a._d.localeCompare(b._d)).map(g=>`<tr><td>${fD(g._d)}</td><td>${esc(g.descripcion)}</td><td><span class="tag tg">${(g.cat||'').slice(0,5)}</span>${g.fuera_caja?' <span class="tag to">local</span>':''}</td><td>${$m(g.amount)}</td></tr>`).join('')||`<tr><td colspan="4" class="empty-row">Sin gastos operativos</td></tr>`;
  const comprasMes=S.co.filter(c=>c.day.startsWith(rMonth)).map(c=>`<tr><td>${fD(c.day)}</td><td>${esc(c.proveedor)}</td><td><span class="tag to">compra</span></td><td>${$m(c.total)}</td></tr>`).join('');
  return`
  <div class="mtabs">${mthTabs}</div>
  <div class="kpis t3">
    <div class="kc hi" onclick="toggleDetail('det-vm')" style="cursor:pointer"><div class="kl">Ventas ▾</div><div class="kv a">${$m(tv)}</div><div class="kh">click para ver</div></div>
    <div class="kc" onclick="toggleDetail('det-gm')" style="cursor:pointer"><div class="kl">Gastos ▾</div><div class="kv r">${$m(tg+tCompras)}</div><div class="kh" style="color:var(--tx3)">op. ${$m(tg)} · comp. ${$m(tCompras)}</div></div>
    <div class="kc"><div class="kl">Resultado</div><div class="kv ${resultado>=0?'g':'r'}">${$m(resultado)}</div></div>
  </div>
  <div id="det-vm" style="display:none">
    <div class="tbk"><div class="tt">Ventas por grupo — ${fM(rMonth)}</div><div class="tbk-hint">→ deslizá para ver Costo y Margen</div><div class="tbk-scroll"><table><thead><tr><th>Grupo</th><th>Cantidad</th><th>Total</th><th>Efectivo</th><th>Transf.</th><th>Costo</th><th>Margen</th></tr></thead><tbody>${bgRows}</tbody></table></div></div>
  </div>
  <div id="det-gm" style="display:none">
    ${tg>0?`<div class="tbk"><div class="tt">Gastos operativos — ${fM(rMonth)}</div><table><thead><tr><th>Fecha</th><th>Descripción</th><th>Cat.</th><th>Monto</th></tr></thead><tbody>${gsRows}</tbody></table></div>`:''}
    ${comprasMes?`<div class="tbk"><div class="tt">Compras del mes — ${fM(rMonth)}</div><table><thead><tr><th>Fecha</th><th>Proveedor</th><th>Tipo</th><th>Total</th></tr></thead><tbody>${comprasMes}</tbody></table></div>`:''}
    ${!tg&&!comprasMes?`<div style="font-size:11px;color:var(--tx3);font-family:var(--mo);padding:8px 0">Sin gastos ni compras</div>`:''}
  </div>
  <div class="kpis t3">
    <div class="kc"><div class="kl">Efectivo total</div><div class="kv g" style="font-size:14px">${$m(tvEf+ingEf-egEf)}</div></div>
    <div class="kc"><div class="kl">Digital total</div><div class="kv b" style="font-size:14px">${$m(tvTr+ingTr-egTr)}</div></div>
  </div>
  ${blkRetiros(rMonth,resultado)}
  <div class="blk"><div class="bt">Ventas diarias — ${fM(rMonth)}</div><div class="ch-w"><canvas id="cM"></canvas></div></div>
  <button class="btn btng" onclick="exportExcel()" style="width:100%;margin-top:6px">⬇ Exportar todo a Excel</button>`;
}

function rRepAnual(yr){
  const an=yrData(yr);const totV=an.reduce((s,x)=>s+x.tv,0),totG=an.reduce((s,x)=>s+x.tg,0),totRes=an.reduce((s,x)=>s+x.res,0);
  const rows=an.map(m=>`<tr><td>${m.lbl}</td><td style="font-family:var(--mo);color:var(--ac)">${$m(m.tv)}</td><td style="font-family:var(--mo);color:var(--rd)">${$m(m.tg)}</td><td style="font-family:var(--mo);font-weight:500;color:${m.res>=0?'var(--gn)':'var(--rd)'}">${$m(m.res)}</td></tr>`).join('');
  return`
  <div class="kpis t3"><div class="kc hi"><div class="kl">Ventas ${yr}</div><div class="kv a">${$m(totV)}</div></div><div class="kc"><div class="kl">Gastos ${yr}</div><div class="kv r">${$m(totG)}</div></div><div class="kc"><div class="kl">Resultado</div><div class="kv ${totRes>=0?'g':'r'}">${$m(totRes)}</div></div></div>
  <div class="blk"><div class="bt">Ventas vs Gastos — ${yr}</div><div class="ch-w" style="height:155px"><canvas id="cA"></canvas></div></div>
  <div class="tbk"><div class="tt">Detalle mensual ${yr}</div><table><thead><tr><th>Mes</th><th>Ventas</th><th>Gastos</th><th>Resultado</th></tr></thead><tbody>${rows}</tbody></table></div>
  <button class="btn btng" onclick="exportExcel()" style="width:100%;margin-top:6px">⬇ Exportar todo a Excel</button>`;
}

function setCmpA(m){cmpA=m;go('reportes')}
function setCmpB(m){cmpB=m;go('reportes')}
function setCmpMetric(v){cmpMetric=v;go('reportes')}
// Diferencia % entre dos valores — devuelve texto con signo, o "—" si no hay base (a=0) para calcular
function pctTxt(a,b){if(!a)return b?'—':'0%';const p=Math.round(((b-a)/Math.abs(a))*100);return(p>=0?'+':'')+p+'%'}
function rRepComparar(mths){
  if(mths.length<2)return`<div class="tbk"><div style="padding:20px 10px;text-align:center;color:var(--tx3);font-size:11px;font-family:var(--mo)">Necesitás al menos 2 meses con datos cargados para poder comparar.</div></div>`;
  if(!cmpA||!mths.includes(cmpA))cmpA=mths[1]||mths[0];
  if(!cmpB||!mths.includes(cmpB)||cmpB===cmpA)cmpB=mths.find(m=>m!==cmpA)||mths[0];
  const selA=`<select onchange="setCmpA(this.value)" style="width:100%">${mths.map(m=>`<option value="${m}" ${m===cmpA?'selected':''}>${fM(m)}</option>`).join('')}</select>`;
  const selB=`<select onchange="setCmpB(this.value)" style="width:100%">${mths.map(m=>`<option value="${m}" ${m===cmpB?'selected':''}>${fM(m)}</option>`).join('')}</select>`;
  const kA=finKpis(cmpA),kB=finKpis(cmpB);
  const kpiRow=(lbl,va,vb,fmt=$m)=>`<tr><td>${lbl}</td><td style="font-family:var(--mo)">${fmt(va)}</td><td style="font-family:var(--mo)">${fmt(vb)}</td><td style="font-family:var(--mo);font-weight:600;color:${vb>=va?'var(--gn)':'var(--rd)'}">${vb>=va?'+':''}${fmt(vb-va)}</td><td style="font-family:var(--mo);color:var(--tx3)">${pctTxt(va,vb)}</td></tr>`;
  const kpiRows=kpiRow('Ventas',kA.tv,kB.tv)+kpiRow('Gastos totales',kA.tGastoTotal,kB.tGastoTotal)+kpiRow('Resultado',kA.resultado,kB.resultado)+kpiRow('Margen',kA.margen,kB.margen,v=>v+'%')+kpiRow('Efectivo total',kA.efectivoTotal,kB.efectivoTotal)+kpiRow('Digital total',kA.digitalTotal,kB.digitalTotal);

  const byGA=mData(cmpA).byG,byGB=mData(cmpB).byG;
  const gNames=[...new Set([...Object.keys(byGA),...Object.keys(byGB)])];
  const esMonto=cmpMetric==='monto';
  const fVal=(n,u)=>esMonto?$m(n):fQ(n,u);
  const gRows=gNames.map(n=>{
    const ua=byGA[n]?.unit,ub=byGB[n]?.unit,u=ua||ub;
    const a=esMonto?(byGA[n]?.tot||0):(byGA[n]?.qty||0);
    const b=esMonto?(byGB[n]?.tot||0):(byGB[n]?.qty||0);
    return{n,a,b,u,dif:b-a};
  }).sort((x,y)=>Math.abs(y.dif)-Math.abs(x.dif))
    .map(x=>`<tr><td>${esc(x.n)}</td><td style="font-family:var(--mo)">${fVal(x.a,x.u)}</td><td style="font-family:var(--mo)">${fVal(x.b,x.u)}</td><td style="font-family:var(--mo);font-weight:600;color:${x.dif>=0?'var(--gn)':'var(--rd)'}">${x.dif>=0?'+':''}${fVal(x.dif,x.u)}</td><td style="font-family:var(--mo);color:var(--tx3)">${pctTxt(x.a,x.b)}</td></tr>`).join('')||`<tr><td colspan="5" class="empty-row">Sin ventas en ninguno de los dos meses</td></tr>`;
  const metricToggle=`<div class="mtabs" style="margin-bottom:0"><button class="mtab ${esMonto?'active':''}" onclick="setCmpMetric('monto')">$ Monto</button><button class="mtab ${!esMonto?'active':''}" onclick="setCmpMetric('qty')">⚖ Cantidad</button></div>`;

  const catA=catGastosMes(cmpA,kA.tCompras),catB=catGastosMes(cmpB,kB.tCompras);
  const cNames=[...new Set([...Object.keys(catA),...Object.keys(catB)])];
  const cRows=cNames.map(n=>{const a=catA[n]||0,b=catB[n]||0;return{n,a,b,dif:b-a}}).sort((x,y)=>Math.abs(y.dif)-Math.abs(x.dif))
    .map(x=>`<tr><td>${esc(x.n)}</td><td style="font-family:var(--mo)">${$m(x.a)}</td><td style="font-family:var(--mo)">${$m(x.b)}</td><td style="font-family:var(--mo);font-weight:600;color:${x.dif<=0?'var(--gn)':'var(--rd)'}">${x.dif>=0?'+':''}${$m(x.dif)}</td><td style="font-family:var(--mo);color:var(--tx3)">${pctTxt(x.a,x.b)}</td></tr>`).join('')||`<tr><td colspan="5" class="empty-row">Sin gastos en ninguno de los dos meses</td></tr>`;

  return`
  <div class="fr"><div class="fl"><label>Mes A</label>${selA}</div><div class="fl"><label>Mes B</label>${selB}</div></div>
  <div class="blk"><div class="bt">${fM(cmpA)} vs ${fM(cmpB)}</div><div class="ch-w" style="height:155px"><canvas id="cCmp"></canvas></div></div>
  <div class="tbk"><div class="tt">Resumen financiero</div><div class="tbk-hint">→ deslizá para ver la variación</div><div class="tbk-scroll"><table><thead><tr><th></th><th>${fM(cmpA)}</th><th>${fM(cmpB)}</th><th>Var. $</th><th>Var. %</th></tr></thead><tbody>${kpiRows}</tbody></table></div></div>
  <div class="tbk"><div class="tt" style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px">Ventas por grupo — qué explica la diferencia${metricToggle}</div><div class="tbk-hint">→ ordenado por mayor variación</div><div class="tbk-scroll"><table><thead><tr><th>Grupo</th><th>${fM(cmpA)}</th><th>${fM(cmpB)}</th><th>Var. ${esMonto?'$':'cant.'}</th><th>Var. %</th></tr></thead><tbody>${gRows}</tbody></table></div></div>
  <div class="tbk"><div class="tt">Gastos por categoría</div><div class="tbk-hint">→ ordenado por mayor variación</div><div class="tbk-scroll"><table><thead><tr><th>Categoría</th><th>${fM(cmpA)}</th><th>${fM(cmpB)}</th><th>Var. $</th><th>Var. %</th></tr></thead><tbody>${cRows}</tbody></table></div></div>`;
}

// Diferencias de caja (faltante/sobrante) acumuladas en el mes — para Reportes financieros
// Evolución de caja día por día del mes — fondo, movimientos y saldo, todo junto
function evolucionCajaMes(ym){
  const dias=Object.keys(S.cierres||{}).filter(d=>d.startsWith(ym)).sort();
  return dias.map(d=>{
    const c=S.cierres[d];
    const vs=S.ve[d]||[];const{ef}=calcEfTr(vs);
    const compraGastoIdsM=idsGastoCompra();
    const gaEf=(S.ga[d]||[]).filter(g=>esGastoCaja(g,compraGastoIdsM)).reduce((s,g)=>s+gastoEf(g),0);
    const movs=S.caja[d]||[];
    const ingEf=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
    const egEf=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
    const prevDay=getPrevDay(d);
    const fondo=c.fondo_inicial_manual!=null?c.fondo_inicial_manual:(S.cierres?.[prevDay]?.saldo_siguiente||0);
    const deberia=fondo+ef+(ingEf-egEf)-gaEf-(c.retiro||0);
    const contado=Math.max(0,c.total_contado-(c.retiro||0));
    return{d,fondo,ef,movManual:ingEf-egEf,gaEf,retiro:c.retiro||0,deberia,contado,dif:contado-deberia,saldo:c.saldo_siguiente};
  });
}
function cierreDiffsForMonth(ym){
  const dias=Object.keys(S.cierres||{}).filter(d=>d.startsWith(ym)).sort();
  let totalDif=0;const detalle=[];
  dias.forEach(d=>{
    const c=S.cierres[d];
    const vs=S.ve[d]||[];const{ef}=calcEfTr(vs);
    const compraGastoIdsM=idsGastoCompra();
    const gaEf=(S.ga[d]||[]).filter(g=>esGastoCaja(g,compraGastoIdsM)).reduce((s,g)=>s+gastoEf(g),0);
    const movs=S.caja[d]||[];
    const ingEf=movs.filter(m=>m.tipo==='ingreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
    const egEf=movs.filter(m=>m.tipo==='egreso'&&m.metodo==='efectivo').reduce((s,m)=>s+m.monto,0);
    const prevDay=getPrevDay(d);
    const fondo=c.fondo_inicial_manual!=null?c.fondo_inicial_manual:(S.cierres?.[prevDay]?.saldo_siguiente||0);
    const deberia=fondo+ef+(ingEf-egEf)-gaEf-(c.retiro||0);
    const contadoNeto=Math.max(0,c.total_contado-(c.retiro||0));
    const dif=contadoNeto-deberia;
    totalDif+=dif;
    if(Math.abs(dif)>=50)detalle.push({d,dif});
  });
  return{totalDif,detalle,diasCerrados:dias.length};
}
function rRepFin(mthTabs){
  const{tv,tvEf,tvTr,tg,tgLocal,tCompras,ingEf,ingTr,egEf,egTr,ingExtra,resultado,tGastoTotal,ingresoTotal,margen,egTotal,efectivoTotal,digitalTotal}=finKpis(rMonth);
  const margenCol=margen>30?'var(--gn)':margen>10?'var(--ac)':'var(--rd)';
  const catGas=catGastosMes(rMonth,tCompras);
  const catRows=Object.entries(catGas).sort((a,b)=>b[1]-a[1]).map(([c,v])=>`<tr><td>${c}</td><td style="font-family:var(--mo)">${$m(v)}</td><td style="font-family:var(--mo);color:var(--tx3)">${Math.round(tGastoTotal>0?(v/tGastoTotal)*100:0)}%</td></tr>`).join('')||`<tr><td colspan="3" class="empty-row">Sin gastos</td></tr>`;
  const{totalDif,detalle:difDetalle,diasCerrados}=cierreDiffsForMonth(rMonth);
  const difRows=difDetalle.map(x=>`<tr><td>${x.d.slice(8,10)}/${x.d.slice(5,7)}</td><td style="font-family:var(--mo);color:${x.dif>=0?'var(--gn)':'var(--rd)'}">${x.dif>=0?'+':''}${$m(x.dif)}</td></tr>`).join('')||`<tr><td colspan="2" class="empty-row">Sin diferencias relevantes</td></tr>`;
  const evo=evolucionCajaMes(rMonth);
  const evoTableRows=evo.map(r=>`<tr><td>${r.d.slice(8,10)}/${r.d.slice(5,7)}</td><td style="font-family:var(--mo)">${$m(r.fondo)}</td><td style="font-family:var(--mo);color:var(--gn)">${$m(r.ef)}</td><td style="font-family:var(--mo);color:${r.movManual>=0?'var(--gn)':'var(--rd)'}">${r.movManual>=0?'+':''}${$m(r.movManual)}</td><td style="font-family:var(--mo);color:var(--rd)">-${$m(r.gaEf)}</td><td style="font-family:var(--mo);color:var(--rd)">-${$m(r.retiro)}</td><td style="font-family:var(--mo);font-weight:600">${$m(r.deberia)}</td><td style="font-family:var(--mo)">${$m(r.contado)}</td><td style="font-family:var(--mo);color:${Math.abs(r.dif)<50?'var(--gn)':r.dif>0?'var(--ac)':'var(--rd)'}">${r.dif>=0?'+':''}${$m(r.dif)}</td><td style="font-family:var(--mo);font-weight:600;color:var(--bl)">${$m(r.saldo)}</td></tr>`).join('')||`<tr><td colspan="10" class="empty-row">Sin cierres confirmados este mes</td></tr>`;
  const evoRows=`<div class="tbk"><div class="tt">Evolución de caja — día por día</div><div class="tbk-hint">→ deslizá para ver toda la fila</div><div class="tbk-scroll"><table><thead><tr><th>Día</th><th>Fondo inicial</th><th>Ventas ef.</th><th>Mov. manual</th><th>Gastos</th><th>Retiro</th><th>Debería haber</th><th>Contado</th><th>Diferencia</th><th>Saldo siguiente</th></tr></thead><tbody>${evoTableRows}</tbody></table></div></div>`;
  return`
  <div class="mtabs">${mthTabs}</div>
  <div class="kpis t3"><div class="kc hi"><div class="kl">Ingresos totales</div><div class="kv a">${$m(ingresoTotal)}</div><div class="kh">ventas + extra</div></div><div class="kc"><div class="kl">Gastos totales</div><div class="kv r">${$m(tGastoTotal)}</div><div class="kh" style="font-size:9px;color:var(--tx3)">caja ${$m(tg-tgLocal)} · local ${$m(tgLocal)} · comp. ${$m(tCompras)}</div></div><div class="kc"><div class="kl">Resultado</div><div class="kv ${resultado>=0?'g':'r'}">${$m(resultado)}</div></div></div>
  <div class="kpis t3"><div class="kc"><div class="kl">Margen</div><div class="kv" style="color:${margenCol}">${margen}%</div></div><div class="kc"><div class="kl">Efectivo total</div><div class="kv g" style="font-size:14px">${$m(efectivoTotal)}</div></div><div class="kc"><div class="kl">Digital total</div><div class="kv b" style="font-size:14px">${$m(digitalTotal)}</div></div></div>
  ${ingExtra>0||egTotal>0?`<div class="blk"><div class="bt">Movimientos de caja del período</div><div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--br)"><span style="font-size:11px;color:var(--tx2)">Ingresos extra</span><span style="font-family:var(--mo);color:var(--gn)">${$m(ingExtra)}</span></div><div style="display:flex;justify-content:space-between;padding:5px 0"><span style="font-size:11px;color:var(--tx2)">Egresos extra</span><span style="font-family:var(--mo);color:var(--rd)">${$m(egTotal)}</span></div></div>`:''}
  ${blkRetiros(rMonth,resultado)}
  <div class="tbk"><div class="tt">Gastos operativos por categoría</div><table><thead><tr><th>Categoría</th><th>Monto</th><th>%</th></tr></thead><tbody>${catRows}</tbody></table></div>
  ${diasCerrados>0?`<div class="blk"><div class="bt">Diferencias de caja del mes (faltante/sobrante)</div><div style="display:flex;justify-content:space-between;padding:5px 0"><span style="font-size:11px;color:var(--tx2)">Acumulado (${diasCerrados} día${diasCerrados===1?'':'s'} cerrado${diasCerrados===1?'':'s'})</span><span style="font-family:var(--mo);font-weight:600;color:${totalDif>=0?'var(--gn)':'var(--rd)'}">${totalDif>=0?'+':''}${$m(totalDif)}</span></div></div><div class="tbk"><table><thead><tr><th>Día</th><th>Diferencia</th></tr></thead><tbody>${difRows}</tbody></table></div>`:''}
  ${evoRows}
  <button class="btn btng" onclick="exportExcel()" style="width:100%;margin-top:6px">⬇ Exportar todo a Excel</button>`;
}

function initCharts(){
  const ym=rMonth,yr=ym.split('-')[0];const[y2,mo]=ym.split('-').map(Number);const dc=new Date(y2,mo,0).getDate();
  const _cg=idsGastoCompra();
  const labs=[],dV=[],dG=[];for(let d=1;d<=dc;d++){const ds=ym+'-'+d.toString().padStart(2,'0');labs.push(d);dV.push((S.ve[ds]||[]).filter(x=>x.pago!=='cuenta_corriente').reduce((s,x)=>s+x.total,0));dG.push((S.ga[ds]||[]).filter(x=>!_cg.has(x.id)).reduce((s,x)=>s+x.amount,0)+S.co.filter(c=>c.day===ds).reduce((s,c)=>s+c.total,0));}
  const OPTS={responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#8a8680',font:{size:9,family:'DM Mono'}}}},scales:{x:{ticks:{color:'#4e4b48',font:{size:8}},grid:{color:'#252525'}},y:{ticks:{color:'#4e4b48',font:{size:9},callback:v=>'$'+Math.round(v).toLocaleString('es-AR')},grid:{color:'#252525'}}}};
  const cm=document.getElementById('cM');if(cm&&window.Chart){try{if(charts.cM)charts.cM.destroy()}catch(e){}charts.cM=new Chart(cm,{type:'bar',data:{labels:labs,datasets:[{label:'Ventas',data:dV,backgroundColor:'rgba(17,17,17,.7)',borderRadius:3},{label:'Gastos',data:dG,backgroundColor:'rgba(248,113,113,.45)',borderRadius:3}]},options:OPTS});}
  const ca=document.getElementById('cA');if(ca&&window.Chart){try{if(charts.cA)charts.cA.destroy()}catch(e){}const an=yrData(yr);charts.cA=new Chart(ca,{type:'line',data:{labels:an.map(x=>x.lbl),datasets:[{label:'Ventas',data:an.map(x=>x.tv),borderColor:'rgba(17,17,17,.9)',backgroundColor:'rgba(17,17,17,.07)',tension:.3,fill:true,pointRadius:3,borderWidth:2},{label:'Gastos',data:an.map(x=>x.tg),borderColor:'rgba(248,113,113,.7)',backgroundColor:'transparent',tension:.3,pointRadius:3,borderWidth:1.5,borderDash:[4,3]}]},options:OPTS});}
  const cc=document.getElementById('cCmp');if(cc&&window.Chart&&cmpA&&cmpB){try{if(charts.cCmp)charts.cCmp.destroy()}catch(e){}const kA=finKpis(cmpA),kB=finKpis(cmpB);charts.cCmp=new Chart(cc,{type:'bar',data:{labels:['Ventas','Gastos','Resultado'],datasets:[{label:fM(cmpA),data:[kA.tv,kA.tGastoTotal,kA.resultado],backgroundColor:'rgba(148,148,148,.5)',borderRadius:3},{label:fM(cmpB),data:[kB.tv,kB.tGastoTotal,kB.resultado],backgroundColor:'rgba(17,17,17,.75)',borderRadius:3}]},options:OPTS});}
}

/* ══ EXCEL ══════════════════════════════════════════════════════ */
function exportExcel(){
  if(!window.XLSX){alert('Librería cargando, intentá en unos segundos');return;}
  const wb=XLSX.utils.book_new();
  const va=[['Fecha','Hora','Ticket','Cliente','Grupo','Variante','Cant.','Stock usado','Unidad','Precio unit.','Desc %','Total','Pago','Cta.Cte. cobrada']];
  Object.entries(S.ve).sort(([a],[b])=>a.localeCompare(b)).forEach(([d,vs])=>vs.forEach(v=>{const vr=S.vr.find(x=>x.id===v.variant_id),gr=S.sg.find(x=>x.id===v.group_id);va.push([fDL(d),v.time||'',v.ticket_id||v.id,nombreCliente(v.cliente_id||v.cliente_cc_id),gr?.name||'',vr?.name||'',v.qty,v.stock_used||0,gr?.unit||'',v.price_unit,v.descuento_pct||0,v.total,v.pago,v.cliente_cc_id?(v.cc_pagado?'Sí':'No'):'']);}));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(va),'Ventas');
  const _cg=idsGastoCompra();
  const ga=[['Fecha','Hora','Descripción','Categoría','Monto','Tipo','Método']];Object.entries(S.ga).sort(([a],[b])=>a.localeCompare(b)).forEach(([d,gs])=>gs.forEach(g=>ga.push([fDL(d),g.time||'',g.descripcion,g.cat,g.amount,_cg.has(g.id)?'Compra':g.fuera_caja?'Local':'Caja',g.metodo||''])));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(ga),'Gastos');
  const ca2=[['Fecha','Hora','Proveedor','Nro Factura','Total','Efectivo','Transferencia','Nota']];S.co.forEach(c=>ca2.push([fDL(c.day),c.time||'',c.proveedor,c.nro_factura||'',c.total,c.pago_efectivo||0,c.pago_transferencia||0,c.note||'']));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(ca2),'Compras');
  const cta=[['Fecha','Hora','Corte','Grupo','Cantidad','Unidad','Costo/kg']];S.ct.forEach(c=>{S.cti.filter(i=>i.corte_id===c.id).forEach(i=>cta.push([fDL(c.day),c.time||'',c.nombre,i.nombre,i.qty,i.unit||'',i.cost_unit_aplicado||0]));});
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(cta),'Cortes');
  const ela=[['Fecha','Hora','Elaboración','Producto','Kg salida','Rinde (kg/kg MP)','Ingrediente','Tipo','Cantidad','Unidad','Costo ref.']];S.el.forEach(e=>{const its=S.eli.filter(i=>i.elaboracion_id===e.id),r=rindeElab(e.output_qty,its),og=S.sg.find(x=>x.id===e.output_group_id);its.forEach(i=>ela.push([fDL(e.day),e.time||'',e.nombre,og?.name||'',e.output_qty||0,r?+r.toFixed(3):'',i.nombre,i.tipo,i.qty,i.unit||'',i.costo_subtotal||0]));});
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(ela),'Elaboraciones');
  const cja=[['Fecha','Hora','Tipo','Descripción','Método','Monto','Cliente']];Object.entries(S.caja).sort(([a],[b])=>a.localeCompare(b)).forEach(([d,ms])=>ms.forEach(m=>cja.push([fDL(d),m.time||'',m.tipo,m.descripcion,m.metodo,m.monto,nombreCliente(m.cliente_cc_id)])));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(cja),'Caja');
  // Resumen: los gastos totales incluyen compras, igual que el Resultado
  const rm=[['Mes','Ventas (sin Cta.Cte.)','Cobros Cta.Cte.','Efectivo','Transferencias','Gastos de caja','Gastos del local','Compras','Gastos totales','Resultado','Mis retiros (costo, aparte)']];
  getMths().forEach(ym=>{const d=mData(ym);rm.push([fM(ym),d.tv,d.cobrosCC,d.tvEf,d.tvTr,d.tg-d.tgLocal,d.tgLocal,d.tCompras,d.tg+d.tCompras,d.resultado,retirosPeriodo(x=>x.startsWith(ym)).costo]);});
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(rm),'Resumen mensual');
  const rtx=[['Fecha','Hora','Producto','Cantidad','Stock usado','Unidad','Costo/u','Costo total','Valor venta','Nota']];(S.rt||[]).slice().sort((a,b)=>String(a.day).localeCompare(String(b.day))).forEach(r=>rtx.push([fDL(String(r.day).slice(0,10)),r.time||'',r.nombre,r.qty,r.stock_used,r.unit||'',r.costo_unit||0,r.costo_total||0,r.valor_venta||0,r.note||'']));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(rtx),'Mis retiros');
  const sta=[['Nombre','Tipo','Unidad','Stock actual','Costo/u']];S.sg.forEach(g=>sta.push([g.name,g.tipo||'venta',g.unit||'kg',g.stock_qty||0,g.cost_unit||0]));
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(sta),'Stock');
  const cli=[['Cliente','Teléfono','Saldo Cta.Cte.','Última compra','Kg últimos 30 días']];S.ccl.forEach(c=>{const r=resumenCliente(c.id);cli.push([c.nombre,c.telefono||'',saldoClienteCC(c.id),r.ult?fDL(r.ult):'',+r.kg30.toFixed(2)]);});
  XLSX.utils.book_append_sheet(wb,XLSX.utils.aoa_to_sheet(cli),'Clientes');
  XLSX.writeFile(wb,`TheChickenCoop_${arDay().replace(/-/g,'')}.xlsx`);toast('Excel descargado ✓');
}

/* ══ BACKUP ══════════════════════════════════════════════════════
   Baja TODAS las tablas directo de Supabase a un archivo .json.
   Es la copia de seguridad: guardala fuera del celular (Drive, mail).    */
const TABLAS_BACKUP=['usuarios','stock_groups','stock_variants','insumos','clientes_cc','cliente_precios','ventas','caja_movimientos','compras','compras_items','cortes','cortes_items','elaboraciones','elaboraciones_items','gastos','cierres','retiros_stock'];
async function exportBackup(){
  if(OB.length&&!confirm(`Hay ${OB.length} registro(s) que todavía no subieron. El backup baja lo que está en la nube. ¿Seguir igual?`))return;
  toast('Bajando backup...');
  try{
    const datos={generado:new Date().toISOString(),proyecto:'vmsjwvudqxfadrcasexu',tablas:{}};
    for(const t of TABLAS_BACKUP){try{datos.tablas[t]=await sbQ(t,'select=*&order=id');}catch(e){if(t!=='cliente_precios'&&t!=='retiros_stock')throw e;}}
    const a=document.createElement('a');
    a.href=URL.createObjectURL(new Blob([JSON.stringify(datos)],{type:'application/json'}));
    a.download=`TCC_backup_${datos.generado.slice(0,16).replace(/[-:T]/g,'')}.json`;
    document.body.appendChild(a);a.click();a.remove();
    const n=Object.values(datos.tablas).reduce((s,x)=>s+x.length,0);
    LC.s('ultimo_backup',arDay());
    toast(`Backup descargado ✓ (${n} registros)`);render();
  }catch(e){alert('No se pudo hacer el backup: '+(e.message||e)+'\nRevisá la conexión y probá de nuevo.');}
}

/* ══ BOOT ══════════════════════════════════════════════════════ */
function pantalla(id){['device-screen','login-screen','app-screen'].forEach(x=>{const el=document.getElementById(x);if(el)el.style.display=x===id?(x==='app-screen'?'block':'flex'):'none';});}
function arrancar(){
  if(!auGet()){pantalla('device-screen');const em=document.getElementById('dv-email');if(em&&!em.value)em.focus();return;}
  if(sesion){pantalla('app-screen');if(appIniciada){flush();loadAll();}else initApp();return;}
  pantalla('login-screen');
  sbQ('usuarios','order=id').then(us=>{if(us?.length){S.us=us;LC.s('us',us);}}).catch(()=>{});
}
async function vincularDispositivo(){
  const em=document.getElementById('dv-email'),pw=document.getElementById('dv-pass'),er=document.getElementById('dv-error'),bt=document.getElementById('dv-btn');
  const email=(em.value||'').trim().toLowerCase(),pass=pw.value||'';
  if(!email||!pass){er.textContent='Completá mail y contraseña';return;}
  bt.disabled=true;er.style.color='var(--tx3)';er.textContent='Conectando...';
  try{
    const r=await fetch(SB+'/auth/v1/token?grant_type=password',{method:'POST',headers:{apikey:SK,'Content-Type':'application/json'},body:JSON.stringify({email,password:pass})});
    const j=await r.json().catch(()=>({}));
    if(!r.ok){er.style.color='';er.textContent=r.status===400?'Mail o contraseña incorrectos':'No se pudo ingresar ('+(j.msg||j.error_description||r.status)+')';return;}
    auGuardar(j);pw.value='';er.textContent='';
    toast('Dispositivo vinculado ✓');
    arrancar();
  }catch(e){er.style.color='';er.textContent='Sin conexión. Para vincular el dispositivo hace falta internet (una sola vez).';}
  finally{bt.disabled=false;}
}
function pedirReingreso(){const em=document.getElementById('dv-email');if(em)em.value=auGet()?.email||em.value;LC.s('auth',null);arrancar();}
async function desvincularDispositivo(){
  if(sesion?.rol!=='dueno')return;
  const pend=OB.length+OBerr.length;
  if(!confirm('¿Cerrar la sesión de ESTE dispositivo?\n\nPara volver a usar la app acá vas a tener que ingresar el mail y la contraseña del local.'+(pend?`\n\nOJO: hay ${pend} registro(s) sin subir. Quedan guardados acá y suben cuando vuelvas a ingresar.`:'')))return;
  const a=auGet();
  if(a?.access_token)fetch(SB+'/auth/v1/logout',{method:'POST',headers:{apikey:SK,Authorization:'Bearer '+a.access_token}}).catch(()=>{});
  LC.s('auth',null);sesion=null;SC.s('sesion',null);arrancar();
}
arrancar();
