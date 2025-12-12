def main():
    st.set_page_config(page_title="Procesador Web de Reportes de Proveedores", layout="centered")

    # Inicializar el autenticador
    authenticator = stauth.Authenticate(
        config_yaml['credentials'],
        config_yaml['cookie']['name'],
        config_yaml['cookie']['key'],
        config_yaml['cookie']['expiry_days']
    )

    # --- Mostrar el formulario de inicio de sesión ---
    # ¡LÍNEA CORREGIDA! Solo pasamos el título/nombre del formulario.
    name, authentication_status, username = authenticator.login('Inicio de Sesión') 

    if authentication_status:
        # 1. ESTADO: Autenticado
        st.session_state['name'] = name # Guardar el nombre del usuario en la sesión
        st.sidebar.markdown(f"**Bienvenido/a:** {name}")
        authenticator.logout('Cerrar Sesión', 'sidebar') # Botón de cierre en la barra lateral
        app_content() # Mostrar el contenido principal de la aplicación
        
# ... (resto del script sigue igual) ...