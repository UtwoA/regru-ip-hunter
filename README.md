# Reg.ru IP Hunter

Утилита поиска нужных IP для Reg.ru Cloud.

## Что делает
- создаёт reglet в выбранном регионе Reg.ru
- проверяет выданный IP на совпадение с целью
- если IP не подходит, удаляет reglet и продолжает поиск
- если IP подходит, останавливается и сохраняет найденный результат
- отправляет уведомление в Telegram

## Что нужно для запуска
- API token Reg.ru Cloud
- Telegram bot token
- Telegram user/chat id для уведомлений
- Docker и Docker Compose

## Быстрый старт
1. Скопируйте `.env.example` в `.env`
2. Заполните переменные окружения
3. Запустите:

```bash
docker compose up --build -d
```

Логи:

```bash
docker compose logs -f
```

## Данные аккаунта
Для каждого аккаунта указываются:
- имя аккаунта
- API token
- target IP или маска
- регионы для перебора

## Поддерживаемые форматы цели
- `1.2.3.4` — точный IP
- `1.2` — префикс
- `1.*.*.4` — wildcard
- `1.2.3.0/24` — CIDR
- `1.2, 5.6` — несколько целей через запятую
---

## Поддержать проект

Если хотите поддержать проект — спасибо, это помогает развивать канал и выкладывать новые материалы.

**Криптовалюта · USDT (TRC20)**

| | |
|:--|:--|
| Сеть | TRC20 |
| Кошелёк | TFvdDeAANV5is6cWEuNXuhhxjBhhedgiWP |

**Через Telegram:** [@UmbrellaDonateBot](https://t.me/UmbrellaDonateBot)

---

[@Umbrella_Free](https://t.me/Umbrella_Free) · [UmbrellaDevs](https://github.com/UmbrellaDevs)
