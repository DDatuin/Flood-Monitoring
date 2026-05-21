from django.urls import path
from .views import get_emergency_contacts, get_latest_data, get_place_details, get_safe_route, get_sensor_history, get_user_weather_info, get_vehicle_thresholds, get_web_chart_history, get_latest_specific_sensor_water_level_data, search_places

urlpatterns = [
    path('latest-data/', get_latest_data),
    path('latest-specific/', get_latest_specific_sensor_water_level_data),
    path('history/', get_sensor_history),
    path('web-history/', get_web_chart_history),
    path('route/', get_safe_route),
    path('vehicle-thresholds/', get_vehicle_thresholds),
    path('emergency/', get_emergency_contacts),
    path('user-weather/', get_user_weather_info),
    path('location-search/', search_places),
    path('location-details/', get_place_details),
]