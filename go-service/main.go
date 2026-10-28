package main

import (
    "context"
    "database/sql"
    "fmt"
    "math"
    "net/http"
    "time"

    "github.com/afex/hystrix-go/hystrix"
    "github.com/gin-gonic/gin"
    "github.com/go-redis/redis/v8"
    _ "github.com/go-sql-driver/mysql"
    "github.com/Shopify/sarama"
)

var (
    db            *sql.DB
    rdb           *redis.Client
    kafkaProducer sarama.SyncProducer
    ctx           = context.Background()
)

type CityNode struct {
    Code string
    Name string
    X, Y float64
}

type Route struct {
    ID            string    `json:"id"`
    FromCity      string    `json:"from_city"`
    ToCity        string    `json:"to_city"`
    TransportType int       `json:"transport_type"`
    DepartTime    time.Time `json:"depart_time"`
    ArriveTime    time.Time `json:"arrive_time"`
    Price         int       `json:"price"`
}

type AStarNode struct {
    CityCode string
    CostG    float64
    CostH    float64
    CostF    float64
    Parent   *AStarNode
    Route    *Route
}

func initMysql() {
    var err error
    dsn := "root:Schedule@123456@tcp(mysql-master.schedule-system:3306)/schedule_db?charset=utf8mb4&parseTime=True&loc=Local"
    db, err = sql.Open("mysql", dsn)
    if err != nil {
        panic(fmt.Sprintf("MySQL连接失败：%v", err))
    }
    db.SetMaxOpenConns(100)
    db.SetMaxIdleConns(20)
    db.SetConnMaxLifetime(1 * time.Hour)
    db.SetConnMaxIdleTime(30 * time.Minute)
    if err = db.Ping(); err != nil {
        panic(fmt.Sprintf("MySQL ping失败：%v", err))
    }
    fmt.Println("✅ MySQL连接池初始化成功")
}

func initRedis() {
    rdb = redis.NewFailoverClient(&redis.FailoverOptions{
        MasterName:       "redis-master",
        SentinelAddrs:    []string{
            "redis-0.redis-headless:26379",
            "redis-1.redis-headless:26379",
            "redis-2.redis-headless:26379",
        },
        Password:    "Redis@123456",
        SentinelPassword: "Redis@123456",
        PoolSize:    200,
        MinIdleConns: 20,
        MaxRetries:  2,
        DialTimeout: 3 * time.Second,
        ReadTimeout: 1 * time.Second,
        WriteTimeout: 1 * time.Second,
    })
    _, err := rdb.Ping(ctx).Result()
    if err != nil {
        panic(fmt.Sprintf("Redis连接失败：%v", err))
    }
    fmt.Println("✅ Redis连接池初始化成功")
}

func initKafka() {
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Return.Successes = true
    config.Producer.Retry.Max = 2
    var err error
    kafkaProducer, err = sarama.NewSyncProducer([]string{"kafka-service.schedule-system:9092"}, config)
    if err != nil {
        panic(fmt.Sprintf("Kafka生产者初始化失败：%v", err))
    }
    fmt.Println("✅ Kafka生产者初始化成功")
}

func initHystrix() {
    hystrix.ConfigureCommand("query_route", hystrix.CommandConfig{
        Timeout:                3000,
        MaxConcurrentRequests:  1000,
        ErrorPercentThreshold:  50,
        SleepWindow:            5000,
        RequestVolumeThreshold: 20,
    })
    fmt.Println("✅ 熔断器初始化成功")
}

func AStarSearch(fromCity, toCity string, strategy int, cityMap map[string]CityNode, routeMap map[string][]*Route) ([]*Route, error) {
    startNode, ok := cityMap[fromCity]
    if !ok {
        return nil, fmt.Errorf("出发城市不存在")
    }
    endNode, ok := cityMap[toCity]
    if !ok {
        return nil, fmt.Errorf("到达城市不存在")
    }

    openList := make(map[string]*AStarNode)
    closeList := make(map[string]bool)

    start := &AStarNode{
        CityCode: fromCity,
        CostG:    0,
        CostH:    heuristic(startNode, endNode, strategy),
    }
    start.CostF = start.CostG + start.CostH
    openList[fromCity] = start

    for len(openList) > 0 {
        var current *AStarNode
        minF := math.MaxFloat64
        for _, node := range openList {
            if node.CostF < minF {
                minF = node.CostF
                current = node
            }
        }

        if current.CityCode == toCity {
            var path []*Route
            for current.Parent != nil {
                path = append([]*Route{current.Route}, path...)
                current = current.Parent
            }
            return path, nil
        }

        delete(openList, current.CityCode)
        closeList[current.CityCode] = true

        routes, ok := routeMap[current.CityCode]
        if !ok {
            continue
        }

        for _, route := range routes {
            nextCity := route.ToCity
            if closeList[nextCity] {
                continue
            }

            var costG float64
            switch strategy {
            case 1:
                costG = current.CostG + route.ArriveTime.Sub(route.DepartTime).Minutes()
            case 2:
                costG = current.CostG + float64(route.Price)
            case 3:
                costG = current.CostG + 1
            }

            nextCityNode := cityMap[nextCity]
            nextNode := &AStarNode{
                CityCode: nextCity,
                CostG:    costG,
                CostH:    heuristic(nextCityNode, endNode, strategy),
                Parent:   current,
                Route:    route,
            }
            nextNode.CostF = nextNode.CostG + nextNode.CostH

            if exist, ok := openList[nextCity]; ok && nextNode.CostG >= exist.CostG {
                continue
            }
            openList[nextCity] = nextNode
        }
    }
    return nil, fmt.Errorf("未找到可行路径")
}

func heuristic(current, target CityNode, strategy int) float64 {
    distance := math.Sqrt(math.Pow(current.X-target.X, 2) + math.Pow(current.Y-target.Y, 2))
    switch strategy {
    case 1:
        return distance / 800 * 60
    case 2:
        return distance * 0.5
    case 3:
        return 1
    default:
        return 0
    }
}

func queryRouteHandler(c *gin.Context) {
    var req struct {
        FromCity string `json:"from_city" binding:"required"`
        ToCity   string `json:"to_city" binding:"required"`
        Strategy int    `json:"strategy" binding:"required,oneof=1 2 3"`
    }
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "参数错误", "error": err.Error()})
        return
    }

    var result gin.H
    err := hystrix.Do("query_route", func() error {
        cacheKey := fmt.Sprintf("route:%s:%s:%d", req.FromCity, req.ToCity, req.Strategy)
        cacheResult, err := rdb.Get(ctx, cacheKey).Result()
        if err == nil {
            result = gin.H{"code": 200, "msg": "查询成功", "data": cacheResult, "from": "cache"}
            return nil
        }

        cityMap, routeMap, err := loadCityAndRouteData()
        if err != nil {
            return err
        }

        path, err := AStarSearch(req.FromCity, req.ToCity, req.Strategy, cityMap, routeMap)
        if err != nil {
            return err
        }

        rdb.Set(ctx, cacheKey, path, 5*time.Minute)
        result = gin.H{"code": 200, "msg": "查询成功", "data": path, "from": "database"}
        return nil
    }, func(err error) error {
        cacheKey := fmt.Sprintf("route:%s:%s:%d", req.FromCity, req.ToCity, req.Strategy)
        if cacheData, e := rdb.Get(ctx, cacheKey).Result(); e == nil {
            result = gin.H{"code": 200, "msg": "系统繁忙，返回缓存数据", "data": cacheData, "from": "fallback_cache"}
            return nil
        }
        result = gin.H{"code": 503, "msg": "系统繁忙，请稍后再试"}
        return err
    })

    if err != nil && result == nil {
        c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "查询失败"})
        return
    }
    c.JSON(http.StatusOK, result)
}

func loadCityAndRouteData() (map[string]CityNode, map[string][]*Route, error) {
    cityMap := make(map[string]CityNode)
    rows, err := db.Query("SELECT code, name, x, y FROM cities")
    if err != nil {
        return nil, nil, err
    }
    defer rows.Close()
    for rows.Next() {
        var city CityNode
        if err := rows.Scan(&city.Code, &city.Name, &city.X, &city.Y); err != nil {
            return nil, nil, err
        }
        cityMap[city.Code] = city
    }

    routeMap := make(map[string][]*Route)
    rows2, err := db.Query("SELECT id, from_city, to_city, transport_type, depart_time, arrive_time, price FROM routes")
    if err != nil {
        return nil, nil, err
    }
    defer rows2.Close()
    for rows2.Next() {
        var route Route
        var departStr, arriveStr string
        if err := rows2.Scan(&route.ID, &route.FromCity, &route.ToCity, &route.TransportType, &departStr, &arriveStr, &route.Price); err != nil {
            return nil, nil, err
        }
        route.DepartTime, _ = time.Parse("2006-01-02 15:04:05", departStr)
        route.ArriveTime, _ = time.Parse("2006-01-02 15:04:05", arriveStr)
        routeMap[route.FromCity] = append(routeMap[route.FromCity], &route)
    }
    return cityMap, routeMap, nil
}

func main() {
    initMysql()
    initRedis()
    initKafka()
    initHystrix()

    r := gin.Default()
    r.GET("/health", func(c *gin.Context) {
        c.String(http.StatusOK, "ok")
    })
    api := r.Group("/api/v1")
    {
        api.POST("/route/query", queryRouteHandler)
    }

    fmt.Println("🚀 班次查询服务启动，端口8080")
    if err := r.Run(":8080"); err != nil {
        panic(err)
    }
}
