## Парсинг фитнес-центров в городах с google-maps и классифицация по площади (>=1000 м2 и <1000 м2)
Задача: отфильтровать фитнес объекты с большой площадью, потеряв как можно меньше

Процесс парсинга продемонстрирован на примере Джакарта
1) Город разбивается на тайлы 1.5 км, для каждого тайла создается свой контейнер в docker и выкачиваются фитнес центры по заданным параметрам:
- zoom = 17
- depth = 15
- radius = 1100
- keywords: fitness, gym, yoga studio, pilates studio
2) Делаем сетку 0.75 на 0.75 км для каждого тайла создается свой контейнер в docker, задаем параметры:
- zoom = 17
- depth = 15
- radius = 600
- keywords: fitness, gym, yoga studio, pilates studio
- режим fast mode
3) Так как fast mode выкачивает не полную информацию по объектам, берем объекты, которых нет в первой выгрузке, запускаем для них парсер, чтобы выкачать все характеристики, задаем параметры:
-  zoom = 18
-  depth = 2
-  radius = 350
-  на каждую точку создаем свой keywords: {title}, fitness, gym, yoga studio, pilates studio
4) Объединяем все файлы, удаляем дубликаты и обрезаем по маске города
5) В итоговом файле получаем:
- Количество отзывов
- Студия, наличие бассейна, сауны
- Фильтрация изображений на полезные и бесполезные [openai/clip-vit-large-patch14](https://huggingface.co/openai/clip-vit-large-patch14)
- Классификация изображений по размеру помещения [openai/clip-vit-large-patch14](https://huggingface.co/openai/clip-vit-large-patch14)
- Применение модели описания фотографий [nlpconnect/vit-gpt2-image-captioning](https://huggingface.co/nlpconnect/vit-gpt2-image-captioning)
- Подсчет глубины изображений [depth-anything/Depth-Anything-V2-Base-hf](https://huggingface.co/depth-anything/Depth-Anything-V2-Base-hf)
- Число дней существования объекта на google-maps, с помощью [парсинга даты самого раннего отзыва](https://github.com/uroplatus666/fitness_area/blob/master/reviews_dates_loading.ipynb)
- [Площадь здания из OSM](https://github.com/uroplatus666/fitness_area/blob/master/area_build_osm.ipynb), в котором находится объект, если его координаты пересекаются с каким-то зданием OSM

**HistGradientBoostingClassifier** 
+ условия

### [Обучающая выборка](https://github.com/uroplatus666/fitness_area/blob/master/classify/train_turkey.xlsx): 
- г. Анкара
- г. Бурса
- г. Истанбул

### Метрики:
- Accuracy: 0.94
- Precision: 0.74
- Recall: 0.70
- F1 Score: 0.72

### Пример реализации:
- г. Джакарта
